package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.vertx.mutiny.sqlclient.Tuple;

public class OmitTerm extends ProjectionTerm {

	private Set<String> allTopLevelAttribs;

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
		tuple.addArrayOfString(allTopLevelAttribs.toArray(new String[0]));
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
			tuple.addArrayOfString(allTopLevelAttribs.toArray(new String[0]));
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
		query.append("NOT (ARRAY(SELECT jsonb_object_keys(ENTITY)) <@ $");
		query.append(dollar);
		dollar++;
		HashSet<String> tmp = Sets.newHashSet(getAllTopLevelAttribs());
		tmp.add(NGSIConstants.NGSI_LD_CREATED_AT);
		tmp.add(NGSIConstants.NGSI_LD_MODIFIED_AT);
		tuple.addArrayOfString(tmp.toArray(new String[0]));
		query.append(')');
		return dollar;
	}

	@Override
	public boolean calculateEntity(Map<String, Object> entity) {
		ProjectionTerm current = this;
		while (current != null) {
			if (current.hasLinked) {
				Object attribObj = entity.get(current.attrib);
				if (attribObj != null) {
					if (attribObj instanceof List<?> attrList) {
						for (Object attrInstanceObj : attrList) {
							if (attrInstanceObj instanceof Map<?, ?> instanceMap
									&& instanceMap.containsKey(NGSIConstants.NGSI_LD_ENTITY)) {
								List<Map<String, Object>> entities = (List<Map<String, Object>>) instanceMap
										.get(NGSIConstants.NGSI_LD_ENTITY);
								Iterator<Map<String, Object>> it = entities.iterator();
								while (it.hasNext()) {
									Map<String, Object> linkedEntity = it.next();
									current.linkedChild.calculateEntity(linkedEntity);
									if(linkedEntity.isEmpty()) {
										it.remove();
									}
								}
								if (entities.isEmpty()) {
									instanceMap.remove(NGSIConstants.NGSI_LD_ENTITY);
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
		return true;
	}

	private Set<String> getAllTopLevelAttribs() {
		if (allTopLevelAttribs == null) {
			allTopLevelAttribs = Sets.newHashSet();
			ProjectionTerm current = this;
			while (current != null) {
				if (!current.hasLinked) {
					allTopLevelAttribs.add(current.attrib);
				}
				current = current.next;
			}
		}
		return allTopLevelAttribs;

	}

}
