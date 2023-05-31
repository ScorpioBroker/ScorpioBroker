CREATE TABLE public.entitymap
(
    "q_token" text NOT NULL,
    "entity_id" text,
	"remote_hosts" jsonb,
	"order_field" numeric NOT NULL
);

CREATE INDEX i_entitymap_qtoken
    ON public.entitymap USING hash
    ("q_token" text_pattern_ops)
;

CREATE TABLE public.entitymap_management
(
    q_token text NOT NULL,
    last_access timestamp with time zone NOT NULL,
    PRIMARY KEY (q_token)
);
