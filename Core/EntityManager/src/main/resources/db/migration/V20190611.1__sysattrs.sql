-- entity
ALTER TABLE entity ALTER data DROP NOT NULL;
ALTER TABLE entity ADD data_without_sysattrs JSONB;

-- csource
ALTER TABLE csource ALTER data DROP NOT NULL;
ALTER TABLE csource ADD data_without_sysattrs JSONB;
