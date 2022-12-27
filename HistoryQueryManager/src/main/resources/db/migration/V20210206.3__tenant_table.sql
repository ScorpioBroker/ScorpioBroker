CREATE TABLE IF NOT EXISTS tenant (
  tenant_id TEXT NOT NULL,
  database_name varchar(255) UNIQUE,
  PRIMARY KEY (tenant_id)
);