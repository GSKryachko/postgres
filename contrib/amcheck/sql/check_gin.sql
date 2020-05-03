-- minimal test, basically just verifying that amcheck works with GiST
SELECT setseed(1);
CREATE TABLE "gin_check"("Column1" int[]);
INSERT INTO gin_check select array_agg(round(random()*100)) from generate_series(1, 1000000) as i group by i % 100000;
INSERT INTO gin_check select array_agg(100 + round(random()*100)) from generate_series(1, 100) as i group by i % 100;
CREATE INDEX gin_check_idx on "gin_check" USING GIN("Column1");
SELECT gin_index_parent_check('gin_check_idx');

-- cleanup
DROP TABLE gin_check;
