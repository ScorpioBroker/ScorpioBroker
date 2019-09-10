\pset pager 0
\set ECHO queries
\timing

-- performance evaluation: IN vs JOIN

EXPLAIN ANALYZE
SELECT data FROM csource 
WHERE id IN (SELECT csource_id from csourceinformation 
              WHERE entity_type = 'http://example.org/vehicle/Vehicle' AND
                    entity_id = 'urn:ngsi-ld:Vehicle:AP3000');
-- 108 ms

EXPLAIN ANALYZE
SELECT c.data FROM csource c
INNER JOIN csourceinformation ci ON (ci.csource_id = c.id)
WHERE ci.entity_type = 'http://example.org/vehicle/Vehicle' AND 
      ci.entity_id = 'urn:ngsi-ld:Vehicle:AP3000';
-- 3 ms

EXPLAIN ANALYZE
SELECT c.data FROM csource c
INNER JOIN csourceinformation ci ON (ci.csource_id = c.id)
WHERE ci.entity_type = 'http://example.org/bus/Bus' AND 
      ci.entity_id IS NULL AND 
      ci.entity_idpattern IS NULL;
-- 25ms

-- discarded. execution plan was exactly the same as "id in (..." approach.
/*EXPLAIN ANALYZE
SELECT c.data FROM csource c
WHERE EXISTS (SELECT csource_id from csourceinformation 
               WHERE entity_type = 'http://example.org/vehicle/Vehicle' AND 
                     entity_id = 'urn:ngsi-ld:Vehicle:AP1000' AND
                     csource_id = c.id);
*/