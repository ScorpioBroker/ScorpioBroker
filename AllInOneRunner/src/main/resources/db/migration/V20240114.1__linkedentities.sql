ALTER TABLE IF EXISTS public.entity
    ADD COLUMN linked_entities jsonb[] NOT NULL DEFAULT array[]::jsonb[];
WITH A AS
	(SELECT ID,
			ENTITY
		FROM ENTITY),
	B AS
	(SELECT A.ID AS ID,
			X.VALUE AS VALUE
		FROM A,
			JSONB_EACH(A.ENTITY) AS X
		WHERE JSONB_TYPEOF(X.VALUE) = 'array'),
c AS (SELECT distinct B.ID as id, Z ->> '@id' as link , 
	jsonb_agg(distinct OBJECTTYPE ->> '@id') as objectTypes
FROM B,
	JSONB_ARRAY_ELEMENTS(B.VALUE) AS Y,
	JSONB_ARRAY_ELEMENTS(Y #> '{https://uri.etsi.org/ngsi-ld/hasObject}') AS Z,
	JSONB_ARRAY_ELEMENTS(Y #> '{https://uri.etsi.org/ngsi-ld/hasObjectType}') AS OBJECTTYPE
WHERE Y #>> '{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship' AND Y ? 'https://uri.etsi.org/ngsi-ld/hasObjectType'
GROUP BY id, link),
D AS (SELECT distinct B.ID as id, Z ->> '@id' as link
FROM B,
	JSONB_ARRAY_ELEMENTS(B.VALUE) AS Y,
	JSONB_ARRAY_ELEMENTS(Y #> '{https://uri.etsi.org/ngsi-ld/hasObject}') AS Z
WHERE Y #>> '{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship' AND NOT Y ? 'https://uri.etsi.org/ngsi-ld/hasObjectType'),
E AS (UPDATE entity SET linked_entities = linked_entities || jsonb_build_object('id', update1.link) FROM (SELECT * FROM D) as update1 WHERE entity.id = update1.id)
UPDATE entity SET linked_entities = linked_entities || jsonb_build_object('id', update2.link, 'objectTypes', update2.objectTypes) FROM (SELECT * FROM C) as update2 WHERE entity.id = update2.id


