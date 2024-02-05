CREATE OR REPLACE FUNCTION NGSILD_APPENDBATCH(ENTITIES jsonb, NOOVERWRITE boolean) RETURNS jsonb AS $ENTITYOAR$
DECLARE
    resultObj jsonb;
    resultEntry jsonb;
    newentity jsonb;
    prev_entity jsonb;
    updated_entity jsonb;
    not_overwriting boolean;
    to_update jsonb;
    to_append jsonb;
BEGIN
    resultObj := '{"success": [], "failure": []}'::jsonb;

    FOR newentity IN SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITIES) LOOP
        prev_entity := NULL;

        BEGIN
            SELECT ENTITY FROM ENTITY WHERE ID = newentity->>'@id' INTO prev_entity;

            SELECT
                jsonb_object_agg(key, Array[(value->0) || jsonb_build_object('https://uri.etsi.org/ngsi-ld/createdAt', prev_entity->key->0->'https://uri.etsi.org/ngsi-ld/createdAt')])::jsonb
                || jsonb_build_object('https://uri.etsi.org/ngsi-ld/modifiedAt', newentity->'https://uri.etsi.org/ngsi-ld/modifiedAt')
            FROM jsonb_each((newentity - '@id' - '@type'))
            WHERE key IN (SELECT jsonb_object_keys(prev_entity))
            INTO to_update;

            IF NOOVERWRITE THEN
                SELECT jsonb_object_agg(key, value)::jsonb
                FROM jsonb_each(newentity)
                WHERE key NOT IN (SELECT jsonb_object_keys(prev_entity))
                INTO to_append;

                IF to_append IS NOT NULL THEN
                    UPDATE ENTITY
                    SET ENTITY = ENTITY || to_append || jsonb_build_object('https://uri.etsi.org/ngsi-ld/modifiedAt', newentity->'https://uri.etsi.org/ngsi-ld/modifiedAt')
                    WHERE ID = newentity->>'@id'
                    RETURNING ENTITY.ENTITY INTO updated_entity;
                ELSE
                    not_overwriting := true;
                END IF;
            ELSIF newentity ? '@type' THEN
                UPDATE ENTITY
                SET E_TYPES = ARRAY(SELECT jsonb_array_elements_text(newentity->'@type')),
                    ENTITY = ENTITY.ENTITY || to_update || CASE WHEN to_append IS NOT NULL THEN to_append ELSE '{}' END
                WHERE ID = newentity->>'@id'
                RETURNING ENTITY.ENTITY INTO updated_entity;
            ELSE
                UPDATE ENTITY
                SET ENTITY = ENTITY.ENTITY || to_update || CASE WHEN to_append IS NOT NULL THEN to_append ELSE '{}' END
                WHERE ID = newentity->>'@id'
                RETURNING ENTITY.ENTITY INTO updated_entity;
            END IF;

            IF not_overwriting THEN
                resultObj['failure'] = resultObj['failure'] || jsonb_build_object(newentity->>'@id', 'Not Overwriting');
            ELSIF NOT FOUND THEN
                resultObj['failure'] = resultObj['failure'] || jsonb_build_object(newentity->>'@id', 'Not Found');
            ELSE
                resultObj['success'] = resultObj['success'] || jsonb_build_object('id', newentity->'@id', 'old', prev_entity, 'new', updated_entity)::jsonb;
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE '%', SQLERRM;
                resultObj['failure'] = resultObj['failure'] || jsonb_build_object(newentity->>'@id', SQLSTATE);
        END;
    END LOOP;

    RETURN resultObj;
END;
$ENTITYOAR$
LANGUAGE PLPGSQL;


ALTER TABLE temporalentityattrinstance ADD COLUMN IF NOT EXISTS static boolean