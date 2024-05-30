package eu.neclab.ngsildbroker.commons.datatypes.terms;

import com.github.jsonldjava.core.Context;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.vertx.core.json.JsonArray;
import io.vertx.mutiny.sqlclient.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringJoiner;

public class DataSetIdTerm implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 7407313014717911008L;
	Set<String> ids = Sets.newHashSet();

	public DataSetIdTerm() {
		// for serialization
	}

	public Set<String> getIds() {
		return ids;
	}

	public void addId(String id) {
		this.ids.add(id);
	}

	public void setIds(Set<String> ids) {
		this.ids = ids;
	}

	public void calculate(List<Map<String, Object>> queryResult) {
		for (Map<String, Object> entity : queryResult) {
			Iterator<Entry<String, Object>> it = entity.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, Object> attrs = it.next();
				String attrsName = attrs.getKey();
				if (NGSIConstants.ENTITY_BASE_PROPS.contains(attrsName)) {
					continue;
				}
				List<Map<String, Object>> attrsValueList = (List<Map<String, Object>>) attrs.getValue();
				Iterator<Map<String, Object>> it2 = attrsValueList.iterator();
				while (it2.hasNext()) {
					Map<String, Object> attrInstance = it2.next();
					Object datasetObj = attrInstance.get(NGSIConstants.NGSI_LD_DATA_SET_ID);
					if (datasetObj == null) {
						if (!ids.contains(NGSIConstants.JSON_LD_NONE)) {
							it2.remove();
						}
					} else {
						String datasetId = ((Map<String, String>) datasetObj).get(NGSIConstants.JSON_LD_ID);
						if (!ids.contains(NGSIConstants.JSON_LD_NONE)) {
							it2.remove();
						}
					}
				}
				if (attrsValueList.isEmpty()) {
					it.remove();
				}
			}
		}
	}

	public int toSql(StringBuilder query, Tuple tuple, int dollar, String tableToUse) {
		query.append(
				"""
						 x AS (
						    WITH RECURSIVE i AS (
						        SELECT
						            __tableToUse.id,
						            __tableToUse.entity,
						            key,
						            to_jsonb(value) AS val
						        FROM
						            __tableToUse,
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
						            '@id', __tableToUse.entity -> '@id',
						            '@type', __tableToUse.entity -> '@type',
						            '@context', __tableToUse.entity -> '@context',
						            'https://uri.etsi.org/ngsi-ld/scope', __tableToUse.entity -> 'https://uri.etsi.org/ngsi-ld/scope',
						            'https://uri.etsi.org/ngsi-ld/createdAt', __tableToUse.entity -> 'https://uri.etsi.org/ngsi-ld/createdAt',
						            'https://uri.etsi.org/ngsi-ld/modifiedAt', __tableToUse.entity -> 'https://uri.etsi.org/ngsi-ld/modifiedAt'
						        )
						    ) || v.entity
						)
						 AS entity
						    FROM
						        v
						    LEFT JOIN
						        __tableToUse ON __tableToUse.id = v.id
						)
						"""
						.formatted(dollar).replace("__tableToUse", tableToUse));
		tuple.addJsonArray(new JsonArray(Lists.newArrayList(ids)));
		dollar++;
		return dollar;
	}

}
