DROP TABLE subscriptions;
DROP TABLE registry_subscriptions;

CREATE TABLE public.subscriptions
(
    subscription_id text,
    subscription jsonb,
    context text,
    PRIMARY KEY (subscription_id)
);

CREATE TABLE public.registry_subscriptions
(
    subscription_id text,
    subscription jsonb,
    context text,
    PRIMARY KEY (subscription_id)
);