CREATE TABLE IF NOT EXISTS registry_subscriptions (
  subscription_id TEXT NOT NULL,
  subscription_request TEXT UNIQUE,
  PRIMARY KEY (subscription_id)
);