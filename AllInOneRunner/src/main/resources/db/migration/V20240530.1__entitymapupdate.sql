DELETE FROM public.entitymap;
DELETE FROM public.entitymap_management;
ALTER TABLE IF EXISTS public.entitymap
    ADD COLUMN "fullentities_dist" boolean;
ALTER TABLE IF EXISTS public.entitymap
    ADD COLUMN "regempty" boolean;
ALTER TABLE IF EXISTS public.entitymap
    ADD COLUMN "noregentry" boolean;
ALTER TABLE IF EXISTS public.entitymap_management
    ADD COLUMN "linked_maps" jsonb;