CREATE TABLE public.entitymap
(
    "q_token" text NOT NULL,
    "entity_id" text,
	"remote_hosts" jsonb,
	"order_field" numeric NOT NULL
);

CREATE INDEX i_entitymap_qtoke
    ON public.entitymap USING hash
    ("entity_id" text_pattern_ops)
;

