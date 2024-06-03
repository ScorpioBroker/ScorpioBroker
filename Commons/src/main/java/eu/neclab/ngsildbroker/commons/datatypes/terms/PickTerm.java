package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Sets;

import java.util.Set;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.vertx.mutiny.sqlclient.Tuple;

public class PickTerm extends ProjectionTerm {

	Set<String> allTopLevelAttribs = null;

	@Override
	protected ProjectionTerm getInstance() {
		return new PickTerm();
	}

	@Override
	public int toSqlConstructEntity(StringBuilder query, Tuple tuple, int dollar, String tableToUse,
			DataSetIdTerm dataSetIdTerm) {
		query.append("JSONB_STRIP_NULLS(JSONB_OBJECT_AGG(");
		query.append(tableToUse);
		query.append(".KEY, CASE WHEN NOY ");
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
		query.append("ENTITY ?| $");
		query.append(dollar);
		dollar++;
		tuple.addArrayOfString(getAllTopLevelAttribs().toArray(new String[0]));
		return dollar;
	}

	private Set<String> getAllTopLevelAttribs() {
		if (allTopLevelAttribs == null) {
			allTopLevelAttribs = Sets.newHashSet();
			ProjectionTerm current = this;
			while (current != null) {
				allTopLevelAttribs.add(current.attrib);
				current = current.next;
			}
		}
		return allTopLevelAttribs;

	}

	@Override
	public void calculateEntity(Map<String, Object> entity) {

		Iterator<Entry<String, Object>> it = entity.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Object> itNext = it.next();
			if (!getAllTopLevelAttribs().contains(itNext.getKey())) {
				it.remove();
			}
		}
	}

}
