CREATE TABLE IF NOT EXISTS subscriptions (
  subscription_id TEXT NOT NULL,
  subscription_request TEXT UNIQUE,
  PRIMARY KEY (subscription_id)
);