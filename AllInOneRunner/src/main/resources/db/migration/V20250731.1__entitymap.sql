DROP TABLE entitymap;

CREATE TABLE entitymap (
    map_id text NOT NULL,
    pos BIGINT NOT NULL,
    query_checksum text NOT NULL,
    entity_id text NOT NULL,
    remote_query text,
    csourceid text NOT NULL,
    last_access timestamp without time zone NOT NULL,
    expires_at timestamp without time zone NOT NULL
);

CREATE INDEX i_entitymap_id ON entitymap USING hash (map_id text_pattern_ops);

CREATE INDEX ON entitymap USING btree (expires_at ASC NULLS LAST)
WITH (deduplicate_items = True);
DROP INDEX "I_entity_types";
CREATE INDEX "I_entity_types" ON public.entity USING gin (e_types array_ops);
REINDEX TABLE entity;
