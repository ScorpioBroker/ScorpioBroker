package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Sets;

import java.util.Set;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.vertx.mutiny.sqlclient.Tuple;

public class PickTerm extends ProjectionTerm {

	String[] allTopLevelAttribs = null;

	@Override
	protected ProjectionTerm getInstance() {
		return new PickTerm();
	}

	@Override
	public int toSqlConstructEntity(StringBuilder query, Tuple tuple, int dollar, String tableToUse,
			DataSetIdTerm dataSetIdTerm) {
		query.append("JSONB_STRIP_NULLS(JSONB_OBJECT_AGG(");
		query.append(tableToUse);
		query.append(".KEY, CASE WHEN NOT ");
		query.append(tableToUse);
		query.append(".KEY = ANY($");
		query.append(dollar);
		dollar++;
		tuple.addArrayOfString(getAllTopLevelAttribs());
		query.append(") THEN NULL ELSE ");
		if (dataSetIdTerm == null) {
			query.append(tableToUse);
			query.append(".VALUE ");
		} else {
			query.append("(SELECT CASE WHEN ");
			query.append(tableToUse);
			query.append(".KEY = ANY($");
			query.append(dollar);
			dollar++;
			tuple.addArrayOfString(NGSIConstants.ENTITY_BASE_PROPS.toArray(new String[0]));
			query.append(dollar);
			dollar++;
			tuple.addArrayOfString(getAllTopLevelAttribs());
			query.append(") THEN ");
			query.append(tableToUse);
			query.append(".VALUE ");
			query.append(
					"WHEN jsonb_array_length(filtered.res) > 0 THEN filtered.res ELSE null END FROM (SELECT jsonb_agg(val) as res FROM jsonb_array_elements(");
			query.append(tableToUse);
			query.append(".VALUE ");
			query.append(") as val where ");
			if (dataSetIdTerm.ids.remove(NGSIConstants.JSON_LD_NONE)) {
				query.append("NOT val ? '");
				query.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
				query.append("'");
				if (!dataSetIdTerm.ids.isEmpty()) {
					query.append(" OR ");
				}
			}
			if (!dataSetIdTerm.ids.isEmpty()) {
				query.append("val ? '");
				query.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
				query.append("' and val #>> '{");
				query.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
				query.append(",0,");
				query.append(NGSIConstants.JSON_LD_ID);
				query.append("}' = ANY($");
				query.append(dollar);
				query.append(")");
				tuple.addArrayOfString(dataSetIdTerm.ids.toArray(new String[0]));
				dollar++;
			}
			query.append(") as filtered)");

		}
		query.append(" END ))");
		return dollar;
	}

	@Override
	public int toSql(StringBuilder query, Tuple tuple, int dollar) {
		query.append("ENTITY ?| $");
		query.append(dollar);
		dollar++;
		tuple.addArrayOfString(getAllTopLevelAttribs());
		return dollar;
	}

	private String[] getAllTopLevelAttribs() {
		if (allTopLevelAttribs == null) {
			HashSet<Object> tmp = Sets.newHashSet();
			ProjectionTerm current = this;
			while (current != null) {
				tmp.add(current.attrib);
				current = current.next;
			}
			allTopLevelAttribs = tmp.toArray(new String[0]);
		}
		return allTopLevelAttribs;

	}

	@Override
	public boolean calculateEntity(Map<String, Object> entity) {
		ProjectionTerm current = this;
		Map<String, Object> result = new HashMap<>(entity.size());
		while (current != null) {
			Object attribObj = entity.get(current.attrib);
			if (attribObj != null) {
				if (current.hasLinked) {
					if (attribObj instanceof List<?> attrList) {
						for (Object attrInstanceObj : attrList) {
							if (attrInstanceObj instanceof Map<?, ?> instanceMap
									&& instanceMap.containsKey(NGSIConstants.NGSI_LD_ENTITY)) {
								List<Map<String, Object>> entities = (List<Map<String, Object>>) instanceMap
										.get(NGSIConstants.NGSI_LD_ENTITY);
								List<Map<String, Object>> resultAttEntityrList = new ArrayList<>(entities.size());
								for (Map<String, Object> linkedEntity : entities) {
									
									if (current.linkedChild
											.calculateEntity(linkedEntity)) {
										resultAttEntityrList.add(linkedEntity);
									}
								}
								if (resultAttEntityrList.isEmpty()) {
									instanceMap.remove(NGSIConstants.NGSI_LD_ENTITY);
								} else {
									((Map<String, Object>) instanceMap).put(NGSIConstants.NGSI_LD_ENTITY,
											resultAttEntityrList);
								}
							}

						}
					}
				}
				result.put(current.attrib, attribObj);
			}
			current = current.next;
		}
		if(result.isEmpty()) {
			return false;
		}
		result.put(NGSIConstants.NGSI_LD_CREATED_AT, entity.get(NGSIConstants.NGSI_LD_CREATED_AT));
		result.put(NGSIConstants.NGSI_LD_MODIFIED_AT, entity.get(NGSIConstants.NGSI_LD_MODIFIED_AT));
		entity.clear();
		entity.putAll(result);
		return true;
	}

}
