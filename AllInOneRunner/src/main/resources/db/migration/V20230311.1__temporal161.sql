ALTER TABLE PUBLIC.temporalentity ADD COLUMN E_TYPES TEXT[];
CREATE INDEX "I_temporalentity_types"
    ON public.entity USING gin
    (e_types array_ops);
UPDATE temporalentity SET E_TYPES=array_append(E_TYPES,TYPE);
ALTER TABLE PUBLIC.temporalentity DROP COLUMN type;
