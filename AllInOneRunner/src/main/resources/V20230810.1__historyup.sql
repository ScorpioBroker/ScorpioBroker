ALTER TABLE IF EXISTS public.temporalentityattrinstance
    ADD COLUMN location geometry;
CREATE INDEX IF NOT EXISTS i_temporalentityattrinstance_location
    ON public.temporalentityattrinstance USING gist
    (location)
    WITH (buffering=auto)
;
CREATE INDEX IF NOT EXISTS i_temporalentityattrinstance_entityid
    ON public.temporalentityattrinstance USING hash
    (temporalentity_id)
;
with x as (SELECT distinct temporalentity_id as eid, geovalue, modifiedat as mat, observedat as oat, COALESCE(modifiedat, observedat) FROM temporalentityattrinstance WHERE geovalue is not null ORDER BY COALESCE(modifiedat, observedat)) UPDATE temporalentityattrinstance SET location = (SELECT x.geovalue FROM x WHERE eid = temporalentity_id and COALESCE(x.mat, x.oat) <= COALESCE(modifiedat, observedat) ORDER BY COALESCE(modifiedat, observedat) DESC limit 1);
