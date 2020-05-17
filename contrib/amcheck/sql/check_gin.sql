-- minimal test, basically just verifying that amcheck works with GiST
SELECT setseed(1);
CREATE TABLE "gin_check"("Column1" int[]);
-- posting trees
INSERT INTO gin_check select array_agg(round(random()*255) ) from generate_series(1, 1000000) as i group by i % 100000;
-- posting leaves
-- INSERT INTO gin_check select array_agg(100 + round(random()*100)) from generate_series(1, 100) as i group by i % 100;
CREATE INDEX gin_check_idx on "gin_check" USING GIN("Column1");
SELECT gin_index_parent_check('gin_check_idx');

-- cleanup
DROP TABLE gin_check;

-- minimal test, basically just verifying that amcheck works with GiST
SELECT setseed(1);
CREATE TABLE "gin_check_text"("Column1" varchar[32]);
-- posting trees
INSERT INTO gin_check_text select array_agg(md5(round(random()*300)::text)) from generate_series(1, 1000000) as i group by i % 100000;
-- posting leaves
-- INSERT INTO gin_check select array_agg(100 + round(random()*100)) from generate_series(1, 100) as i group by i % 100;
CREATE INDEX gin_check_idx_text on "gin_check_text" USING GIN("Column1");
SELECT gin_index_parent_check('gin_check_idx_text');

-- cleanup
DROP TABLE gin_check_text;
