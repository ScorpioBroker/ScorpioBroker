DROP TABLE entitymap;

CREATE TABLE entitymap (
    map_id text NOT NULL,
    query_checksum text NOT NULL,
    entity_id text NOT NULL,
    remote_query text,
    csourceid text NOT NULL,
    last_access time without time zone NOT NULL,
    expires_at time with time zone NOT NULL
);

CREATE INDEX i_entitymap_id ON entitymap USING hash (map_id text_pattern_ops);

CREATE INDEX ON entitymap USING btree (expires_at ASC NULLS LAST)
WITH (deduplicate_items = True);