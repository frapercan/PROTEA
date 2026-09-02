\timing on
SET work_mem = '1GB';
SET max_parallel_workers_per_gather = 4;

-- Ancestor closure in go_id space, including self. is_a and part_of only:
-- those are the relations a GO annotation propagates along.
CREATE TEMP TABLE anc AS
WITH RECURSIVE e AS (
    SELECT pt.go_id AS parent, c.go_id AS child
      FROM go_term_relationship r
      JOIN go_term c  ON c.id = r.child_go_term_id
      JOIN go_term pt ON pt.id = r.parent_go_term_id
     WHERE r.ontology_snapshot_id = '36038118-37ba-4858-8677-f5b5d730bf56'
       AND r.relation_type IN ('is_a','part_of')
),
w(term, up, d) AS (
        SELECT DISTINCT child, child, 0 FROM e
    UNION
        SELECT w.term, e.parent, w.d + 1 FROM w JOIN e ON e.child = w.up WHERE w.d < 20
)
SELECT term, up FROM w;
CREATE INDEX ON anc(term);
SELECT 'anc rows' k, count(*) v FROM anc;

-- The twelve sweep arms. Arm 7 and arm 8 each produced two sets: 124494ac
-- carries a false revision label and c53eda1e covers only 8,192 of the 14,032
-- proteins. Both are excluded. 9995651a is the clean rung, not a sweep arm.
CREATE TEMP TABLE arms(id uuid);
INSERT INTO arms VALUES
 ('e63d4b8d-92f4-4835-98d4-bc029584f818'), ('a741c2cb-87ec-48b0-88c4-42c2b260cee6'),
 ('219ff800-6a90-4eb8-bdcb-8c805ae74083'), ('0b6b7dc9-0723-4a2f-b000-c17ea455dddc'),
 ('baa24ab5-d31f-462d-94fb-5b2104a6cad6'), ('fb30886e-3b9e-45e0-ba74-1a7fb71ca185'),
 ('bfe5daf3-98d2-4d96-b915-d838625181bc'), ('1eb7e6f9-9345-4336-9628-19ea136b4f43'),
 ('cc746c65-4ed6-48d2-9f54-5c8c4448b6e1'), ('8cb6507c-85e4-4a08-a3ac-3768e9d7c403'),
 ('27245d2d-da9a-46aa-8cdb-e1a84f999fcb'), ('eebd27e0-3314-43f2-ac6f-b786b2596eeb');

-- The 14,032 delta proteins, taken from an arm rather than assumed.
CREATE TEMP TABLE q AS
SELECT DISTINCT protein_accession p FROM go_prediction
 WHERE prediction_set_id = 'e63d4b8d-92f4-4835-98d4-bc029584f818';
CREATE INDEX ON q(p);
SELECT 'query proteins' k, count(*)::text v FROM q;

-- Truth and bank, in go_id space, restricted to the queries.
CREATE TEMP TABLE truth_x AS
SELECT DISTINCT a.protein_accession p, t.go_id g
  FROM protein_go_annotation a JOIN go_term t ON t.id = a.go_term_id
  JOIN q ON q.p = a.protein_accession
 WHERE a.annotation_set_id = 'ec9f5c2c-cc1c-4e22-8cda-d1fe53ca86b3'
   AND a.qualifier NOT ILIKE '%NOT%';
CREATE TEMP TABLE bank_x AS
SELECT DISTINCT a.protein_accession p, t.go_id g
  FROM protein_go_annotation a JOIN go_term t ON t.id = a.go_term_id
  JOIN q ON q.p = a.protein_accession
 WHERE a.annotation_set_id = 'cbb35a32-44e4-4e39-b524-05b4b7433727'
   AND a.qualifier NOT ILIKE '%NOT%';

-- Propagated up. This is the space the evaluation actually scores in: a
-- protein annotated with a term is annotated with all of its ancestors.
CREATE TEMP TABLE truth_p AS
SELECT DISTINCT x.p, a.up g FROM truth_x x JOIN anc a ON a.term = x.g;
CREATE TEMP TABLE bank_p AS
SELECT DISTINCT x.p, a.up g FROM bank_x x JOIN anc a ON a.term = x.g;
CREATE INDEX ON bank_p(p, g);

SELECT 'truth exact' k, count(*)::text v FROM truth_x
UNION ALL SELECT 'truth propagated', count(*)::text FROM truth_p
UNION ALL SELECT 'bank exact', count(*)::text FROM bank_x
UNION ALL SELECT 'bank propagated', count(*)::text FROM bank_p;

-- What the window actually adds, both ways of counting it.
CREATE TEMP TABLE new_x AS
SELECT t.p, t.g FROM truth_x t
 WHERE NOT EXISTS (SELECT 1 FROM bank_x b WHERE b.p=t.p AND b.g=t.g);
CREATE TEMP TABLE new_p AS
SELECT t.p, t.g FROM truth_p t
 WHERE NOT EXISTS (SELECT 1 FROM bank_p b WHERE b.p=t.p AND b.g=t.g);
CREATE INDEX ON new_x(p, g);
CREATE INDEX ON new_p(p, g);
SELECT 'new pairs, exact' k, count(*)::text v FROM new_x
UNION ALL SELECT 'new pairs, propagated', count(*)::text FROM new_p;

-- The union of the twelve, and the same union propagated up.
CREATE TEMP TABLE pred_x AS
SELECT DISTINCT g.protein_accession p, t.go_id gg
  FROM go_prediction g JOIN arms ON arms.id = g.prediction_set_id
  JOIN go_term t ON t.id = g.go_term_id;
CREATE TEMP TABLE pred_p AS
SELECT DISTINCT x.p, a.up gg FROM pred_x x JOIN anc a ON a.term = x.gg;
CREATE INDEX ON pred_x(p, gg);
CREATE INDEX ON pred_p(p, gg);
SELECT 'union predicted, exact' k, count(*)::text v FROM pred_x
UNION ALL SELECT 'union predicted, propagated', count(*)::text FROM pred_p;

-- The correction. The original decomposition matched a predicted term against
-- a true term as strings. The evaluation does not: it propagates predictions
-- up the DAG before scoring, so predicting a descendant of a true term reaches
-- that term. Four cells, because both choices matter and only one pairing is
-- the one the scorer sees.
SELECT 'exact pred / exact truth' k, count(*)::text v FROM new_x n
  WHERE EXISTS (SELECT 1 FROM pred_x p WHERE p.p=n.p AND p.gg=n.g)
UNION ALL SELECT 'propagated pred / exact truth', count(*)::text FROM new_x n
  WHERE EXISTS (SELECT 1 FROM pred_p p WHERE p.p=n.p AND p.gg=n.g)
UNION ALL SELECT 'propagated pred / propagated truth', count(*)::text FROM new_p n
  WHERE EXISTS (SELECT 1 FROM pred_p p WHERE p.p=n.p AND p.gg=n.g);

-- Rebuilding the decomposition in the space the scorer works in. A term is
-- available in the bank if any bank protein carries it once the bank is
-- propagated, not if the string appears verbatim.
CREATE TEMP TABLE avail AS SELECT DISTINCT g FROM bank_p;
CREATE INDEX ON avail(g);

CREATE TEMP TABLE missed AS
SELECT n.p, n.g FROM new_x n
 WHERE NOT EXISTS (SELECT 1 FROM pred_p p WHERE p.p=n.p AND p.gg=n.g);
CREATE INDEX ON missed(g);

SELECT 'missed total' k, count(*)::text v FROM missed
UNION ALL SELECT 'missed, term absent from the propagated bank',
       count(*)::text FROM missed m WHERE NOT EXISTS (SELECT 1 FROM avail a WHERE a.g=m.g)
UNION ALL SELECT 'missed, term available and still not reached',
       count(*)::text FROM missed m WHERE EXISTS (SELECT 1 FROM avail a WHERE a.g=m.g);

-- Same question asked of the original, unpropagated bank, which is what the
-- 5.7 per cent cell counted.
SELECT 'absent from the exact bank' k, count(*)::text v FROM missed m
 WHERE NOT EXISTS (SELECT 1 FROM bank_x b WHERE b.g=m.g);

-- One arm against the union, both propagated, so the document can say what a
-- single system reaches and what the twelve reach together.
CREATE TEMP TABLE one_p AS
SELECT DISTINCT g.protein_accession p, a.up gg
  FROM go_prediction g JOIN go_term t ON t.id = g.go_term_id JOIN anc a ON a.term = t.go_id
 WHERE g.prediction_set_id = 'e63d4b8d-92f4-4835-98d4-bc029584f818';
CREATE INDEX ON one_p(p, gg);
SELECT 'one arm reaches (esm2_650m)' k, count(*)::text v FROM new_x n
  WHERE EXISTS (SELECT 1 FROM one_p p WHERE p.p=n.p AND p.gg=n.g);

-- Export the corrected split for the carrier-signal statistic, which was
-- computed on the superseded string-equality split and has to be recomputed
-- on this one or it describes a partition that no longer exists.
\copy (SELECT n.p, n.g, CASE WHEN EXISTS (SELECT 1 FROM pred_p p WHERE p.p=n.p AND p.gg=n.g) THEN 1 ELSE 0 END FROM new_x n) TO '/tmp/allpairs_prop.csv' CSV
