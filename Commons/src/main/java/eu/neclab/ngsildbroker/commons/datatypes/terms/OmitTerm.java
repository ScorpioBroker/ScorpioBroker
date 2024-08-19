package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.mutiny.sqlclient.Tuple;

@RegisterForReflection
public class OmitTerm extends ProjectionTerm {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1506349448320388978L;

	@Override
	protected ProjectionTerm getInstance() {
		return new OmitTerm();
	}

	public static OmitTerm getNewRootInstance() {
		OmitTerm result = new OmitTerm();
//		result.attrib = NGSIConstants.NGSI_LD_CREATED_AT;
//		result = (OmitTerm) result.getNext();
//		result.attrib = NGSIConstants.NGSI_LD_MODIFIED_AT;
		return result;

	}

	private OmitTerm() {

	}

	@Override
	public int toSqlConstructEntity(StringBuilder query, Tuple tuple, int dollar, String tableToUse,
			DataSetIdTerm dataSetIdTerm) {
		query.append("JSONB_STRIP_NULLS(JSONB_OBJECT_AGG(");
		query.append(tableToUse);
		query.append(".KEY, CASE WHEN ");
		query.append(tableToUse);
		query.append(".KEY = ANY($");
		query.append(dollar);
		dollar++;
		tuple.addArrayOfString(getAllTopLevelAttribs(false).toArray(new String[0]));
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
			tuple.addArrayOfString(getAllTopLevelAttribs(false).toArray(new String[0]));
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
		query.append(".KEY, CASE WHEN ");
		query.append(tableToUse);
		query.append(".KEY = ANY(ARRAY[");

		followUp.append("JSONB_STRIP_NULLS(JSONB_OBJECT_AGG(");
		followUp.append(tableToUse);
		followUp.append(".KEY, CASE WHEN ");
		followUp.append(tableToUse);
		followUp.append(".KEY = ANY(ARRAY[''' || ");

		for (String attr : getAllTopLevelAttribs(false)) {
			query.append('$');
			query.append(dollar);
			query.append(',');

			followUp.append('$');
			followUp.append(dollar);
			followUp.append(" || ''',''' || ");
			dollar++;
			tuple.addString(attr);
		}
		query.setLength(query.length() - 1);
		query.append("]) THEN NULL ELSE ");

		followUp.setLength(followUp.length() - 8);
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
				dollar++;
				tuple.addString(attr);
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

				followUp.append("NOT val ? ''");
				followUp.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
				followUp.append("''");
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
					dollar++;
					tuple.addString(attr);
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
		query.append("NOT (ARRAY(SELECT jsonb_object_keys(ENTITY)) <@ $");
		query.append(dollar);
		dollar++;
		HashSet<String> tmp = Sets.newHashSet(getAllTopLevelAttribs(false));
		tmp.add(NGSIConstants.NGSI_LD_CREATED_AT);
		tmp.add(NGSIConstants.NGSI_LD_MODIFIED_AT);
		tuple.addArrayOfString(tmp.toArray(new String[0]));
		query.append(')');
		return dollar;
	}

	public int toSql(StringBuilder query, StringBuilder followUp, Tuple tuple, int dollar) {
		HashSet<String> tmp = Sets.newHashSet(getAllTopLevelAttribs(false));
		tmp.add(NGSIConstants.NGSI_LD_CREATED_AT);
		tmp.add(NGSIConstants.NGSI_LD_MODIFIED_AT);
		query.append("NOT (ARRAY(SELECT jsonb_object_keys(ENTITY)) <@ ARRAY[");
		followUp.append("NOT (ARRAY(SELECT jsonb_object_keys(ENTITY)) <@ ARRAY['''");
		for (String attr : tmp) {
			query.append('$');
			query.append(dollar);
			query.append(',');
			followUp.append(" || $");
			followUp.append(dollar);
			followUp.append(" || ''','''");
			dollar++;
			tuple.addString(attr);
		}
		followUp.setLength(followUp.length() - 4);
		followUp.append("]::text[])");
		query.setCharAt(query.length() - 1, ']');
		query.append("::text[])");
		return dollar;
	}

	@Override
	public boolean calculateEntity(Map<String, Object> entity, boolean flatJoin,
			Map<String, Map<String, Object>> flatEntities, Set<String> pickForFlat, boolean calculateLinked) {
		ProjectionTerm current = this;
		while (current != null) {
			if (current.hasLinked) {
				if (calculateLinked) {
					Object attribObj = entity.get(current.attrib);
					if (attribObj != null) {
						if (attribObj instanceof List<?> attrList) {
							if (!flatJoin) {
								for (Object attrInstanceObj : attrList) {
									if (attrInstanceObj instanceof Map<?, ?> instanceMap
											&& instanceMap.containsKey(NGSIConstants.NGSI_LD_ENTITY)) {
										List<Map<String, Object>> entities = (List<Map<String, Object>>) instanceMap
												.get(NGSIConstants.NGSI_LD_ENTITY);
										Iterator<Map<String, Object>> it = entities.iterator();
										while (it.hasNext()) {
											Map<String, Object> linkedEntity = it.next();
											current.linkedChild.calculateEntity(linkedEntity, flatJoin, flatEntities,
													pickForFlat, calculateLinked);
											if (linkedEntity.isEmpty()) {
												it.remove();
											}
										}
										if (entities.isEmpty()) {
											instanceMap.remove(NGSIConstants.NGSI_LD_ENTITY);
										}
									}

								}
							} else {
								for (Object attrInstanceObj : attrList) {
									if (attrInstanceObj instanceof Map<?, ?> instanceMap
											&& instanceMap.containsKey(NGSIConstants.JSON_LD_TYPE)) {
										String type = ((List<String>) instanceMap.get(NGSIConstants.JSON_LD_TYPE))
												.get(0);
										if (NGSIConstants.NGSI_LD_RELATIONSHIP.equals(type)) {
											Set<String> ids = Sets.newHashSet();
											List<Map<String, String>> objList = (List<Map<String, String>>) instanceMap
													.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
											for (Map<String, String> objEntry : objList) {
												ids.add(objEntry.get(NGSIConstants.JSON_LD_ID));
											}
											for (String id : ids) {
												Map<String, Object> objEntity = flatEntities.get(id);
												if (current.linkedChild.calculateEntity(objEntity, flatJoin,
														flatEntities, pickForFlat, calculateLinked)) {
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
												if (!current.linkedChild.calculateEntity(objEntity, flatJoin,
														flatEntities, pickForFlat, calculateLinked)) {
													pickForFlat.add(id);
												}
											}
										}

									}
								}
							}
						}
					}
				}
			} else {
				entity.remove(current.attrib);
			}
			current = current.next;
		}
		// only createdat and modifiedat are left so it's empty for the result
		if (entity.size() == 2) {
//			entity.remove(NGSIConstants.NGSI_LD_CREATED_AT);
//			entity.remove(NGSIConstants.NGSI_LD_MODIFIED_AT);
			return false;
		}
		if (flatJoin) {
			entity.entrySet().forEach(entry -> {
				Object attribObj = entry.getValue();
				if (attribObj instanceof List<?> attrList) {
					for (Object attrInstanceObj : attrList) {
						if (attrInstanceObj instanceof Map<?, ?> instanceMap
								&& instanceMap.containsKey(NGSIConstants.JSON_LD_TYPE)) {
							String type = ((List<String>) instanceMap.get(NGSIConstants.JSON_LD_TYPE)).get(0);
							if (NGSIConstants.NGSI_LD_RELATIONSHIP.equals(type)) {
								Set<String> ids = Sets.newHashSet();
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
			});
		}
		return true;
	}

}
