/*-------------------------------------------------------------------------
 *
 * verify_gin.c
 *		Verifies the integrity of GIN indexes based on invariants.
 *
 * Verification checks that all paths in GIN graph contain
 * consistent keys: tuples on parent pages consistently include tuples
 * from children pages. Also, verification checks graph invariants:
 * internal page must have at least one downlinks, internal page can
 * reference either only leaf pages or only internal pages.
 *
 *
 * Copyright (c) 2017-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/amcheck/verify_gin.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/gin_private.h"
#include "amcheck.h"
#include "catalog/pg_am.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * GinScanItem represents one item of depth-first scan of GIN index.
 */
typedef struct GinScanItem
{
	int			depth;
	IndexTuple	parenttup;
	BlockNumber parentblk;
	XLogRecPtr	parentlsn;
	BlockNumber blkno;
	struct GinScanItem *next;
} GinScanItem;

PG_FUNCTION_INFO_V1(gin_index_parent_check);

static void gin_index_checkable(Relation rel);
static void gin_check_parent_keys_consistency(Relation rel);
static void check_index_page(Relation rel, Buffer buffer, BlockNumber blockNo);
static IndexTuple gin_refind_parent(Relation rel, BlockNumber parentblkno,
									 BlockNumber childblkno,
									 BufferAccessStrategy strategy);

/*
 * gin_index_parent_check(index regclass)
 *
 * Verify integrity of GIN index.
 *
 * Acquires AccessShareLock on heap & index relations.
 */
Datum gin_index_parent_check(PG_FUNCTION_ARGS)
{
	Oid			indrelid = PG_GETARG_OID(0);
	Relation	indrel;
	Relation	heaprel;
	LOCKMODE	lockmode = AccessShareLock;

	/* lock table and index with neccesary level */
	amcheck_lock_relation(indrelid, &indrel, &heaprel, lockmode);

	/* verify that this is GIN eligible for check */
	gin_index_checkable(indrel);

	if (amcheck_index_mainfork_expected(indrel))
        gin_check_parent_keys_consistency(indrel);

	/* Unlock index and table */
	amcheck_unlock_relation(indrelid, indrel, heaprel, lockmode);

	PG_RETURN_VOID();
}

/*
 * Check that relation is eligible for GIN verification
 */
static void
gin_index_checkable(Relation rel)
{
	if (rel->rd_rel->relkind != RELKIND_INDEX ||
		rel->rd_rel->relam != GIN_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only GIN indexes are supported as targets for this verification"),
				 errdetail("Relation \"%s\" is not a GIN index.",
						   RelationGetRelationName(rel))));

	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions"),
				 errdetail("Index \"%s\" is associated with temporary relation.",
						   RelationGetRelationName(rel))));

	if (!rel->rd_index->indisvalid)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot check index \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail("Index is not valid")));
}

/*
 * Main entry point for GIN check. Allocates memory context and scans through
 * GIN graph.  This function verifies that tuples of internal pages cover all
 * the key space of each tuple on leaf page.  To do this we invoke
 * gin_check_internal_page() for every internal page.
 *
 * giin_check_internal_page() in it's turn takes every tuple and tries to
 * adjust it by tuples on referenced child page.  Parent gin tuple should
 * never require any adjustments.
 */
static void
gin_check_parent_keys_consistency(Relation rel)
{
    BufferAccessStrategy strategy = GetAccessStrategy(BAS_BULKREAD);
	GinScanItem *stack;
	MemoryContext mctx;
	MemoryContext oldcontext;
	int			leafdepth;

	mctx = AllocSetContextCreate(CurrentMemoryContext,
								 "amcheck context",
								 ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(mctx);

	/*
	 * We don't know the height of the tree yet, but as soon as we encounter a
	 * leaf page, we will set 'leafdepth' to its depth.
	 */
	leafdepth = -1;

	/* Start the scan at the root page */
	stack = (GinScanItem *) palloc0(sizeof(GinScanItem));
	stack->depth = 0;
	stack->parenttup = NULL;
	stack->parentblk = InvalidBlockNumber;
	stack->parentlsn = InvalidXLogRecPtr;
	stack->blkno = GIN_ROOT_BLKNO;

	while (stack)
	{
        GinScanItem *stack_next;
		Buffer		buffer;
		Page		page;
		OffsetNumber  i, maxoff;
		XLogRecPtr	lsn;

		CHECK_FOR_INTERRUPTS();

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, stack->blkno,
									RBM_NORMAL, strategy);
		LockBuffer(buffer, GIN_SHARE);
		page = (Page) BufferGetPage(buffer);
		lsn = BufferGetLSNAtomic(buffer);

		/* Do basic sanity checks on the page headers */
		check_index_page(rel, buffer, stack->blkno);

        /*
         * It's possible that the page was split since we looked at the
         * parent, so that we didn't missed the downlink of the right sibling
         * when we scanned the parent.  If so, add the right sibling to the
         * stack now.
         */
        if (!GinPageRightMost(page) &&
            ginCompareItemPointers(&stack->parenttup->t_tid, GinDataPageGetRightBound(page)) >= 0)
        {
            /* split page detected, install right link to the stack */
            GinScanItem *ptr = (GinScanItem *) palloc(sizeof(GinScanItem));

            ptr->depth = stack->depth;
            ptr->parenttup = CopyIndexTuple(stack->parenttup);
            ptr->parentblk = stack->parentblk;
            ptr->parentlsn = stack->parentlsn;
            ptr->blkno = GinPageGetOpaque(page)->rightlink;
            ptr->next = stack->next;
            stack->next = ptr;
        }


		/* Check that the tree has the same height in all branches */
		if (GinPageIsLeaf(page))
		{
			if (leafdepth == -1)
				leafdepth = stack->depth;
			else if (stack->depth != leafdepth)
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("index \"%s\": internal pages traversal encountered leaf page unexpectedly on block %u",
								RelationGetRelationName(rel), stack->blkno)));
		}

		/*
		 * Check that each tuple looks valid, and is consistent with the
		 * downlink we followed when we stepped on this page.
		 */
		maxoff = PageGetMaxOffsetNumber(page);
		for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
		{
			ItemId iid = PageGetItemIdCareful(rel, stack->blkno, page, i, sizeof(GinPageOpaqueData));
			IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);

			if (MAXALIGN(ItemIdGetLength(iid)) != MAXALIGN(IndexTupleSize(idxtuple)))
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("index \"%s\" has inconsistent tuple sizes, block %u, offset %u",
								RelationGetRelationName(rel), stack->blkno, i)));

			/* If this is an internal page, recurse into the child */
			if (!GinPageIsLeaf(page))
			{
				GinScanItem *ptr;

				ptr = (GinScanItem *) palloc(sizeof(GinScanItem));
				ptr->depth = stack->depth + 1;
				ptr->parenttup = CopyIndexTuple(idxtuple);
				ptr->parentblk = stack->blkno;
				ptr->blkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));
				ptr->parentlsn = lsn;
				ptr->next = stack->next;
				stack->next = ptr;
			}
		}

		LockBuffer(buffer, GIN_UNLOCK);
		ReleaseBuffer(buffer);

		/* Step to next item in the queue */
		stack_next = stack->next;
		if (stack->parenttup)
			pfree(stack->parenttup);
		pfree(stack);
		stack = stack_next;
	}

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(mctx);
}

static void
check_index_page(Relation rel, Buffer buffer, BlockNumber blockNo)
{
	Page		page = BufferGetPage(buffer);

//	gistcheckpage(rel, buffer);

	if (GinPageIsDeleted(page))
	{
		if (!GinPageIsLeaf(page))
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("index \"%s\" has deleted internal page %d",
							RelationGetRelationName(rel), blockNo)));
		if (PageGetMaxOffsetNumber(page) > InvalidOffsetNumber)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("index \"%s\" has deleted page %d with tuples",
							RelationGetRelationName(rel), blockNo)));
	}
	else if (PageGetMaxOffsetNumber(page) > MaxIndexTuplesPerPage)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("index \"%s\" has page %d with exceeding count of tuples",
						RelationGetRelationName(rel), blockNo)));
}
