package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Sets;

import java.util.Set;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.mutiny.sqlclient.Tuple;

@RegisterForReflection
public class PickTerm extends ProjectionTerm {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2109841972843886186L;

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
		tuple.addArrayOfString(getAllTopLevelAttribs(true).toArray(new String[0]));
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
			tuple.addArrayOfString(getAllTopLevelAttribs(true).toArray(new String[0]));
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

	public int toSqlConstructEntity(StringBuilder query, StringBuilder followUp, Tuple tuple, int dollar,
			String tableToUse, DataSetIdTerm dataSetIdTerm) {
		query.append("JSONB_STRIP_NULLS(JSONB_OBJECT_AGG(");
		query.append(tableToUse);
		query.append(".KEY, CASE WHEN NOT ");
		query.append(tableToUse);
		query.append(".KEY = ANY(ARRAY[");

		followUp.append("JSONB_STRIP_NULLS(JSONB_OBJECT_AGG(");
		followUp.append(tableToUse);
		followUp.append(".KEY, CASE WHEN NOT ");
		followUp.append(tableToUse);
		followUp.append(".KEY = ANY(ARRAY[''' || ");
		for (String attr : getAllTopLevelAttribs(true)) {
			query.append('$');
			query.append(dollar);
			query.append(',');
			followUp.append('$');
			followUp.append(dollar);
			followUp.append(" || ''',''' || ");
			tuple.addString(attr);
			dollar++;
		}
		query.setLength(query.length() - 1);
		followUp.setLength(followUp.length() - 8);
		query.append("]) THEN NULL ELSE ");
		followUp.append("]) THEN NULL ELSE ");
		if (dataSetIdTerm == null) {
			query.append(tableToUse);
			query.append(".VALUE ");
			followUp.append(tableToUse);
			followUp.append(".VALUE ");
		} else {
			query.append("(SELECT CASE WHEN ");
			query.append(tableToUse);
			query.append(".KEY = ANY(ARRAY[");

			followUp.append("(SELECT CASE WHEN ");
			followUp.append(tableToUse);
			followUp.append(".KEY = ANY(ARRAY[''' || ");
			for (String attr : NGSIConstants.ENTITY_BASE_PROPS) {
				query.append('$');
				query.append(dollar);
				query.append(',');
				followUp.append('$');
				followUp.append(dollar);
				followUp.append(" || ''',''' || ");
				tuple.addString(attr);
				dollar++;
			}
			query.setLength(query.length() - 1);
			query.append("]) THEN ");
			query.append(tableToUse);
			query.append(".VALUE ");
			query.append(
					"WHEN jsonb_array_length(filtered.res) > 0 THEN filtered.res ELSE null END FROM (SELECT jsonb_agg(val) as res FROM jsonb_array_elements(");
			query.append(tableToUse);
			query.append(".VALUE ");
			query.append(") as val where ");

			followUp.setLength(followUp.length() - 8);
			followUp.append("]) THEN ");
			followUp.append(tableToUse);
			followUp.append(".VALUE ");
			followUp.append(
					"WHEN jsonb_array_length(filtered.res) > 0 THEN filtered.res ELSE null END FROM (SELECT jsonb_agg(val) as res FROM jsonb_array_elements(");
			followUp.append(tableToUse);
			followUp.append(".VALUE ");
			followUp.append(") as val where ");
			if (dataSetIdTerm.ids.remove(NGSIConstants.JSON_LD_NONE)) {
				query.append("NOT val ? '");
				query.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
				query.append("'");

				followUp.append("NOT val ? '");
				followUp.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
				followUp.append("'");
				if (!dataSetIdTerm.ids.isEmpty()) {
					query.append(" OR ");
					followUp.append(" OR ");
				}
			}
			if (!dataSetIdTerm.ids.isEmpty()) {
				query.append("val ? '");
				query.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
				query.append("' and val #>> '{");
				query.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
				query.append(",0,");
				query.append(NGSIConstants.JSON_LD_ID);
				query.append("}' = ANY(ARRAY[");

				followUp.append("val ? ''");
				followUp.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
				followUp.append("'' and val #>> ''{");
				followUp.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
				followUp.append(",0,");
				followUp.append(NGSIConstants.JSON_LD_ID);
				followUp.append("}'' = ANY(ARRAY[''' || ");
				for (String attr : dataSetIdTerm.ids) {
					query.append('$');
					query.append(dollar);
					query.append(',');

					followUp.append('$');
					followUp.append(dollar);
					followUp.append(" || ''',''' || ");
					tuple.addString(attr);
					dollar++;
				}
				query.setLength(query.length() - 1);
				query.append("])");

				followUp.setLength(followUp.length() - 8);
				followUp.append("])");
			}
			query.append(") as filtered)");
			followUp.append(") as filtered)");

		}
		query.append(" END ))");
		followUp.append(" END ))");
		return dollar;
	}

	@Override
	public int toSql(StringBuilder query, Tuple tuple, int dollar) {
		query.append("ENTITY ?| $");
		query.append(dollar);
		dollar++;
		tuple.addArrayOfString(getAllTopLevelAttribs(true).toArray(new String[0]));
		return dollar;
	}

	public int toSql(StringBuilder query, StringBuilder followUp, Tuple tuple, int dollar) {
		query.append("ENTITY ?| ARRAY[");
		followUp.append("ENTITY ?| ARRAY['''");
		Set<String> attribs = getAllTopLevelAttribs(true);
		for (String attrib : attribs) {
			query.append('$');
			query.append(dollar);
			query.append(',');
			followUp.append(" || $");
			followUp.append(dollar);
			followUp.append(" || ''','''");
			dollar++;
			tuple.addString(attrib);
		}
		followUp.setLength(followUp.length() - 4);
		followUp.append(']');
		query.setCharAt(query.length() - 1, ']');
		return dollar;
	}

	@Override
	public boolean calculateEntity(Map<String, Object> entity, boolean flatJoin,
			Map<String, Map<String, Object>> flatEntities, Set<String> pickForFlat, boolean calculateLinked) {
		ProjectionTerm current = this;
		Map<String, Object> result = new HashMap<>(entity.size());
		while (current != null) {
			Object attribObj = entity.get(current.attrib);
			if (attribObj != null) {
				if (current.hasLinked && calculateLinked) {
					
					
					if (attribObj instanceof List<?> attrList) {
						if (!flatJoin) {
							for (Object attrInstanceObj : attrList) {
								if (attrInstanceObj instanceof Map<?, ?> instanceMap
										&& instanceMap.containsKey(NGSIConstants.NGSI_LD_ENTITY)) {
									List<Map<String, Object>> entities = (List<Map<String, Object>>) instanceMap
											.get(NGSIConstants.NGSI_LD_ENTITY);
									List<Map<String, Object>> resultAttEntityrList = new ArrayList<>(entities.size());
									for (Map<String, Object> linkedEntity : entities) {

										if (current.linkedChild.calculateEntity(linkedEntity, flatJoin, flatEntities,
												pickForFlat, calculateLinked)) {
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
						} else {
							for (Object attrInstanceObj : attrList) {
								if (attrInstanceObj instanceof Map<?, ?> instanceMap
										&& instanceMap.containsKey(NGSIConstants.JSON_LD_TYPE)) {
									String type = ((List<String>) instanceMap.get(NGSIConstants.JSON_LD_TYPE)).get(0);
									if (NGSIConstants.NGSI_LD_RELATIONSHIP.equals(type)) {
										Set<String> ids = Sets.newHashSet();
										List<Map<String, String>> objList = (List<Map<String, String>>) instanceMap
												.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
										for (Map<String, String> objEntry : objList) {
											ids.add(objEntry.get(NGSIConstants.JSON_LD_ID));
										}
										for (String id : ids) {
											Map<String, Object> objEntity = flatEntities.get(id);
											if (current.linkedChild.calculateEntity(objEntity, flatJoin, flatEntities,
													pickForFlat, calculateLinked)) {
												pickForFlat.add(id);
											}
										}
									} else if (NGSIConstants.NGSI_LD_LISTRELATIONSHIP.equals(type)) {
										Set<String> ids = Sets.newHashSet();
										List<Map<String, List<Map<String, List<Map<String, String>>>>>> objList = (List<Map<String, List<Map<String, List<Map<String, String>>>>>>) instanceMap
												.get(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST);
										for (Map<String, List<Map<String, String>>> objEntry : objList.get(0)
												.get(NGSIConstants.JSON_LD_LIST)) {
											List<Map<String, String>> hasObjList = (List<Map<String, String>>) instanceMap
													.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
											for (Map<String, String> hasObjEntry : hasObjList) {
												ids.add(hasObjEntry.get(NGSIConstants.JSON_LD_ID));
											}
										}
										for (String id : ids) {
											Map<String, Object> objEntity = flatEntities.get(id);
											if (current.linkedChild.calculateEntity(objEntity, flatJoin, flatEntities,
													pickForFlat, calculateLinked)) {
												pickForFlat.add(id);
											}
										}
									}

								}
							}
						}
					}
				} else {
					if (flatJoin) {
						if (attribObj instanceof List<?> attrList) {
							for (Object attrInstanceObj : attrList) {
								if (attrInstanceObj instanceof Map<?, ?> instanceMap
										&& instanceMap.containsKey(NGSIConstants.JSON_LD_TYPE)) {
									String type = ((List<String>) instanceMap.get(NGSIConstants.JSON_LD_TYPE)).get(0);
									if (NGSIConstants.NGSI_LD_RELATIONSHIP.equals(type)) {

										List<Map<String, String>> objList = (List<Map<String, String>>) instanceMap
												.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
										for (Map<String, String> objEntry : objList) {
											pickForFlat.add(objEntry.get(NGSIConstants.JSON_LD_ID));
										}

									} else if (NGSIConstants.NGSI_LD_LISTRELATIONSHIP.equals(type)) {

										List<Map<String, List<Map<String, List<Map<String, String>>>>>> objList = (List<Map<String, List<Map<String, List<Map<String, String>>>>>>) instanceMap
												.get(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST);
										for (Map<String, List<Map<String, String>>> objEntry : objList.get(0)
												.get(NGSIConstants.JSON_LD_LIST)) {
											List<Map<String, String>> hasObjList = (List<Map<String, String>>) instanceMap
													.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
											for (Map<String, String> hasObjEntry : hasObjList) {
												pickForFlat.add(hasObjEntry.get(NGSIConstants.JSON_LD_ID));
											}
										}
									}
								}
							}
						}
					}
				}
				result.put(current.attrib, attribObj);
			}
			current = current.next;
		}
		if (result.isEmpty()) {
			return false;
		}
		result.put(NGSIConstants.NGSI_LD_CREATED_AT, entity.get(NGSIConstants.NGSI_LD_CREATED_AT));
		result.put(NGSIConstants.NGSI_LD_MODIFIED_AT, entity.get(NGSIConstants.NGSI_LD_MODIFIED_AT));
		entity.clear();
		entity.putAll(result);
		return true;
	}

	

}
