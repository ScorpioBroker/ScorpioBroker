CREATE TABLE IF NOT EXISTS public.contexts
(
    id text NOT NULL,
    body jsonb NOT NULL,
    kind text NOT NULL,
    timestmp timestamp without time zone,
    PRIMARY KEY (id)
);
ALTER TABLE public.contexts alter timestmp set default now();
ALTER TABLE IF EXISTS public.contexts
    OWNER to ngb;