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

/*
 * GinPostingTreeScanItem represents one item of depth-first scan of GIN  posting tree.
 */
typedef struct GinPostingTreeScanItem
{
    int			depth;
    PostingItem	*parenttup;
    BlockNumber parentblk;
    BlockNumber blkno;
    struct GinPostingTreeScanItem *next;
} GinPostingTreeScanItem;


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
 * Read item pointers from leaf entry tuple.
 *
 * Returns a palloc'd array of ItemPointers. The number of items is returned
 * in *nitems.
 */
ItemPointer
ginReadTupleWithoutState( IndexTuple itup, int *nitems)
{
    Pointer		ptr = GinGetPosting(itup);
    int			nipd = GinGetNPosting(itup);
    ItemPointer ipd;
    int			ndecoded;

    if (GinItupIsCompressed(itup))
    {
        if (nipd > 0)
        {
            ipd = ginPostingListDecode((GinPostingList *) ptr, &ndecoded);
            if (nipd != ndecoded)
                elog(ERROR, "number of items mismatch in GIN entry tuple, %d in tuple header, %d decoded",
                     nipd, ndecoded);
        }
        else
        {
            ipd = palloc(0);
        }
    }
    else
    {
        ipd = (ItemPointer) palloc(sizeof(ItemPointerData) * nipd);
        memcpy(ipd, ptr, sizeof(ItemPointerData) * nipd);
    }
    *nitems = nipd;
    return ipd;
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
 * Allocates memory context and scans through postigTree graph
 *
 */
static void
gin_check_posting_tree_parent_keys_consistency(Relation rel, BlockNumber posting_tree_root)
{
    BufferAccessStrategy strategy = GetAccessStrategy(BAS_BULKREAD);
    GinPostingTreeScanItem *stack;
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
    stack = (GinPostingTreeScanItem *) palloc0(sizeof(GinPostingTreeScanItem));
    stack->depth = 0;
    stack->parenttup = NULL;
    stack->parentblk = InvalidBlockNumber;
    stack->blkno = posting_tree_root;

    while (stack)
    {
//        elog(INFO, "processing block %u", stack->blkno);
        GinPostingTreeScanItem *stack_next;
        Buffer		buffer;
        Page		page;
        OffsetNumber  i, maxoff;

        CHECK_FOR_INTERRUPTS();

        buffer = ReadBufferExtended(rel, MAIN_FORKNUM, stack->blkno,
                                    RBM_NORMAL, strategy);
        LockBuffer(buffer, GIN_SHARE);
        page = (Page) BufferGetPage(buffer);
//        elog(INFO, "before assert");
        Assert(GinPageIsData(page));
//        elog(INFO, "after assert");

        /* Check that the tree has the same height in all branches */
        if (GinPageIsLeaf(page))
        {
//            elog(INFO, "page is leaf");
            if (leafdepth == -1)
                leafdepth = stack->depth;
            else if (stack->depth != leafdepth)
                ereport(ERROR,
                        (errcode(ERRCODE_INDEX_CORRUPTED),
                                errmsg("index \"%s\": internal pages traversal encountered leaf page unexpectedly on block %u",
                                       RelationGetRelationName(rel), stack->blkno)));
        }
//        elog(INFO, "after checking for leaf assert");

        if (stack->parenttup) {
//            elog(INFO, "parent block u% , parent offset u%", ItemPointerGetBlockNumberNoCheck(&stack->parenttup->key), ItemPointerGetOffsetNumberNoCheck(&stack->parenttup->key) );
        }
//        elog(INFO, "after printing parent info");

        /*
         * Check that tuples in each page are properly ordered and consistent with parent high key
         */
        maxoff = PageGetMaxOffsetNumber(page);
//        elog(INFO, "maxoff %u", (unsigned int)maxoff);
        for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
        {
            PostingItem* posting_item = GinDataPageGetPostingItem(page, i);
            if (ItemPointerGetBlockNumberNoCheck(&posting_item->key) == 0 || ItemPointerGetOffsetNumberNoCheck(&posting_item->key) == 0)
            {
                continue;
            }
//            elog(INFO, "block %u offset %u", ItemPointerGetBlockNumber(&posting_item->key), ItemPointerGetOffsetNumber(&posting_item->key) );
//            if (i!= FirstOffsetNumber){
//                PostingItem* previous_posting_item = GinDataPageGetPostingItem(page, i-1);
//                if (ItemPointerCompare(&posting_item->key, &previous_posting_item->key) < 0 ) {
//                    ereport(ERROR,
//                            (errcode(ERRCODE_INDEX_CORRUPTED),
//                                    errmsg("index \"%s\" has wrong tuple order in posting tree, block %u, offset %u",
//                                           RelationGetRelationName(rel), stack->blkno, i)));
//                }
//
//            }
//            elog(INFO, "Check if this tuple is consistent with the downlink in the");

            /*
             * Check if this tuple is consistent with the downlink in the
             * parent.
             */
//            if (stack->parenttup && i == maxoff) {
//                if (ItemPointerCompare(&stack->parenttup->key, &posting_item->key) < 0) {
//                    ereport(ERROR,
//                            (errcode(ERRCODE_INDEX_CORRUPTED),
//                                    errmsg("index \"%s\" has inconsistent records on page %u offset %u",
//                                           RelationGetRelationName(rel), stack->blkno, i)));
//
//                }
//            }

//            elog(INFO, " If this is an internal page, recurse into the child ");
            /* If this is an internal page, recurse into the child */
            if (!GinPageIsLeaf(page))
            {
                GinPostingTreeScanItem *ptr;

                ptr = (GinPostingTreeScanItem *) palloc(sizeof(GinPostingTreeScanItem));
                ptr->depth = stack->depth + 1;
                ptr->parenttup = posting_item;
                ptr->parentblk = stack->blkno;
                ptr->blkno = BlockIdGetBlockNumber(&posting_item->child_blkno);
                ptr->next = stack->next;
                stack->next = ptr;
            }

        }

//        elog(INFO, " Step to next item in the queue ");
        LockBuffer(buffer, GIN_UNLOCK);
        ReleaseBuffer(buffer);

        /* Step to next item in the queue */
        stack_next = stack->next;
//        elog(INFO, " before freeing parent tup");
//TODO uncomment and fix
//        if (stack->parenttup)
//            pfree(stack->parenttup);

//        elog(INFO, " after freeing parent tup");
        pfree(stack);
//        elog(INFO, " after freeing stack");

        stack = stack_next;
    }
//    elog(INFO, "while ended");

    MemoryContextSwitchTo(oldcontext);
    MemoryContextDelete(mctx);
}

static void validate_leaf(Page page, Relation rel, BlockNumber blkno) {
    OffsetNumber  i, maxoff;
    maxoff = PageGetMaxOffsetNumber(page);
    for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
        ItemId iid = PageGetItemIdCareful(rel, blkno, page, i, sizeof(GinPageOpaqueData));
        IndexTuple idxtuple = (IndexTuple) PageGetItem(page, iid);
        if (GinIsPostingTree(idxtuple)) {
            elog(INFO, "validating posting tree on page %u, block %u, offset %u", page, blkno, i);
            BlockNumber rootPostingTree = GinGetPostingTree(idxtuple);
            gin_check_posting_tree_parent_keys_consistency(rel, rootPostingTree);
        } else {
            elog(INFO, "validating posting list on page %u, block %u, offset %u", page, blkno, i);

            ItemPointer ipd;
            int			nipd;

            ipd = ginReadTupleWithoutState(idxtuple, &nipd);
            if (!OffsetNumberIsValid(ItemPointerGetOffsetNumber(&ipd[nipd-1]))){
                ereport(ERROR,
                        (errcode(ERRCODE_INDEX_CORRUPTED),
                                errmsg("index \"%s\": posting list contains invalid heap pointer on block %u",
                                       RelationGetRelationName(rel), blkno)));
            }
            pfree(ipd);
        }
    }
}

/*
 * Main entry point for GIN check. Allocates memory context and scans through
 * GIN graph.
 */
static void
gin_check_parent_keys_consistency(Relation rel)
{
    BufferAccessStrategy strategy = GetAccessStrategy(BAS_BULKREAD);
	GinScanItem *stack;
	MemoryContext mctx;
	MemoryContext oldcontext;
    GinState  *state;

    int			leafdepth;

	mctx = AllocSetContextCreate(CurrentMemoryContext,
								 "amcheck context",
								 ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(mctx);
    state = (GinState *) palloc(sizeof(GinState));
    initGinState(state, rel);

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
        if (stack -> parenttup != NULL ) {
            GinNullCategory parent_key_category;
            Datum parent_key = gintuple_get_key(state, stack->parenttup, &parent_key_category);
            maxoff = PageGetMaxOffsetNumber(page);
            ItemId iid = PageGetItemIdCareful(rel, stack->blkno, page, maxoff, sizeof(GinPageOpaqueData));
            IndexTuple idxtuple = (IndexTuple) PageGetItem(page, iid);
            OffsetNumber attnum = gintuple_get_attrnum(state, idxtuple);
            GinNullCategory page_max_key_category;
            Datum page_max_key = gintuple_get_key(state, idxtuple, &page_max_key_category);

            if (GinPageGetOpaque(page)->rightlink != InvalidBlockNumber &&
            ginCompareEntries(state, attnum, page_max_key, parent_key, page_max_key_category, parent_key_category) <= 0 ) {
                elog(INFO, "split detected");
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
		 * Check that tuples in each page are properly ordered and consistent with parent high key
		 */
		maxoff = PageGetMaxOffsetNumber(page);
        IndexTuple prev_tuple = NULL;
		for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
		{
			ItemId iid = PageGetItemIdCareful(rel, stack->blkno, page, i, sizeof(GinPageOpaqueData));
			IndexTuple	idxtuple = (IndexTuple) PageGetItem(page, iid);
            OffsetNumber attnum = gintuple_get_attrnum(state, idxtuple);

			if (MAXALIGN(ItemIdGetLength(iid)) != MAXALIGN(IndexTupleSize(idxtuple)))
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("index \"%s\" has inconsistent tuple sizes, block %u, offset %u",
								RelationGetRelationName(rel), stack->blkno, i)));

            GinNullCategory prev_key_category;
            Datum prev_key;
            GinNullCategory current_key_category;
            Datum current_key = gintuple_get_key(state, idxtuple, &current_key_category);

            if (i != FirstOffsetNumber) {
                prev_key = gintuple_get_key(state, prev_tuple, &prev_key_category);

                if (ginCompareEntries(state, attnum, prev_key, current_key, prev_key_category, current_key_category) <= 0)
                    ereport(ERROR,
                            (errcode(ERRCODE_INDEX_CORRUPTED),
                                    errmsg("index \"%s\" has wrong tuple order, block %u, offset %u",
                                           RelationGetRelationName(rel), stack->blkno, i)));

            }

            /*
             * Check if this tuple is consistent with the downlink in the
             * parent.
             */
            if (stack->parenttup &&
                i == maxoff) {
                GinNullCategory parent_key_category;
                Datum parent_key = gintuple_get_key(state, stack->parenttup, &parent_key_category);

                if (ginCompareEntries(state, attnum, current_key, parent_key, current_key_category, parent_key_category) <= 0) {


                    /*
                     * There was a discrepancy between parent and child tuples.
                     * We need to verify it is not a result of concurrent call of
                     * gistplacetopage(). So, lock parent and try to find downlink
                     * for current page. It may be missing due to concurrent page
                     * split, this is OK.
                     */
                    pfree(stack->parenttup);
                    stack->parenttup = gin_refind_parent(rel, stack->parentblk,
                                                         stack->blkno, strategy);

                    /* We found it - make a final check before failing */
                    if (!stack->parenttup)
                        elog(NOTICE, "Unable to find parent tuple for block %u on block %u due to concurrent split",
                             stack->blkno, stack->parentblk);
                    else {
                        parent_key = gintuple_get_key(state, stack->parenttup, &parent_key_category);
                        if (ginCompareEntries(state, attnum, current_key, parent_key, current_key_category, parent_key_category) <=0)
                        ereport(ERROR,
                                (errcode(ERRCODE_INDEX_CORRUPTED),
                                        errmsg("index \"%s\" has inconsistent records on page %u offset %u",
                                               RelationGetRelationName(rel), stack->blkno, i)));
                        else {
                            /*
                             * But now it is properly adjusted - nothing to do here.
                             */
                        }
                    }
                }
            }

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
			} else {
			    validate_leaf(page, rel, stack->blkno);
			}

			prev_tuple = idxtuple;
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

	gincheckpage(rel, buffer);

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

/*
 * Try to re-find downlink pointing to 'blkno', in 'parentblkno'.
 *
 * If found, returns a palloc'd copy of the downlink tuple. Otherwise,
 * returns NULL.
 */
static IndexTuple
gin_refind_parent(Relation rel, BlockNumber parentblkno,
                   BlockNumber childblkno, BufferAccessStrategy strategy)
{
    Buffer		parentbuf;
    Page		parentpage;
    OffsetNumber o,
            parent_maxoff;
    IndexTuple	result = NULL;

    parentbuf = ReadBufferExtended(rel, MAIN_FORKNUM, parentblkno, RBM_NORMAL,
                                   strategy);

    LockBuffer(parentbuf, GIN_SHARE);
    parentpage = BufferGetPage(parentbuf);

    if (GinPageIsLeaf(parentpage))
    {
        UnlockReleaseBuffer(parentbuf);
        return result;
    }

    parent_maxoff = PageGetMaxOffsetNumber(parentpage);
    for (o = FirstOffsetNumber; o <= parent_maxoff; o = OffsetNumberNext(o))
    {
        ItemId p_iid = PageGetItemIdCareful(rel, parentblkno, parentpage, o, sizeof(GinPageOpaqueData));
        IndexTuple	itup = (IndexTuple) PageGetItem(parentpage, p_iid);

        if (ItemPointerGetBlockNumber(&(itup->t_tid)) == childblkno)
        {
            /* Found it! Make copy and return it */
            result = CopyIndexTuple(itup);
            break;
        }
    }

    UnlockReleaseBuffer(parentbuf);

    return result;
}


