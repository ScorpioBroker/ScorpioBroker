CREATE TABLE IF NOT EXISTS public.contexts
(
    id text NOT NULL,
    body jsonb NOT NULL,
    kind text NOT NULL,
    createdat timestamp without time zone,
    PRIMARY KEY (id)
);
ALTER TABLE public.contexts alter createdat set default now();
