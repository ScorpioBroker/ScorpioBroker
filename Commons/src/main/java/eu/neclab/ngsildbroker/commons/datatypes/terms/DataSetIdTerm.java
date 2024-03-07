package eu.neclab.ngsildbroker.commons.datatypes.terms;

import com.github.jsonldjava.core.Context;
import com.google.common.collect.Sets;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.vertx.core.json.JsonArray;
import io.vertx.mutiny.sqlclient.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;

public class DataSetIdTerm implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 7407313014717911008L;
	List<String> ids = new ArrayList<>();

	public DataSetIdTerm() {
		// for serialization
	}

	public List<String> getIds() {
		return ids;
	}

	public void addId(String id) {
		this.ids.add(id);
	}
	public void setIds(List<String> ids) {
		this.ids = ids;
	}

	public int toSql(StringBuilder query, Tuple tuple, int dollar) {
		query.append("""
                 x AS (
                    WITH RECURSIVE i AS (
                        SELECT
                            c.id,
                            c.entity,
                            key,
                            to_jsonb(value) AS val
                        FROM
                            c,
                            jsonb_each(entity)
                        WHERE
                            jsonb_typeof(value) = 'array'
                    ),
                    ii AS (
                        SELECT
                            i.id,
                            i.entity,
                            i.key,
                            jsonb_array_elements(i.val) AS val
                        FROM
                            i
                    ),
                    iii AS (
                        SELECT
                            ii.id,
                            ii.entity,
                            ii.key,
                            ii.val
                        FROM
                            ii
                        WHERE
                            jsonb_typeof(ii.val) = 'object'
                            AND ii.val::jsonb ? 'https://uri.etsi.org/ngsi-ld/datasetId'
                    ),
                    iv AS (
                        SELECT
                            iii.id,
                            iii.entity,
                            iii.key,
                            iii.val
                        FROM
                            iii
                        WHERE
                            iii.val -> 'https://uri.etsi.org/ngsi-ld/datasetId' -> 0 -> '@id' = any (select jsonb_array_elements($%s::jsonb))
                    ),
                    v AS (
                        SELECT
                            iv.id,
                            jsonb_object_agg(iv.key, Array[iv.val]) AS entity
                        FROM
                            iv
                        GROUP BY
                            iv.id,
                            iv.entity
                    )
                    SELECT
                        v.id,
                        json(jsonb_strip_nulls(
				                    jsonb_build_object(
				                        '@id', c.entity -> '@id',
				                        '@type', c.entity -> '@type',
				                        '@context', c.entity -> '@context',
				                        'https://uri.etsi.org/ngsi-ld/scope', c.entity -> 'https://uri.etsi.org/ngsi-ld/scope',
				                        'https://uri.etsi.org/ngsi-ld/createdAt', c.entity -> 'https://uri.etsi.org/ngsi-ld/createdAt',
				                        'https://uri.etsi.org/ngsi-ld/modifiedAt', c.entity -> 'https://uri.etsi.org/ngsi-ld/modifiedAt'
				                    )
				                ) || v.entity
				            )
				             AS entity
                    FROM
                        v
                    LEFT JOIN
                        c ON c.id = v.id
                )
                """.formatted(dollar));
				tuple.addJsonArray(new JsonArray(ids));
		dollar++;
//		query.append(dollar);
//		dollar++;
//		tuple.addArrayOfString(attrs.toArray(new String[0]));
		return dollar;
	}

}
