package eu.neclab.ngsildbroker.historyquerymanager.repository;

import java.util.ArrayList;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AggrTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TemporalQueryTerm;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;

@Singleton
public class HistoryDAO {

	@Inject
	ClientManager clientManager;

	public Uni<RowSet<Row>> retrieveEntity(String tenant, String entityId, AttrsQueryTerm attrsQuery,
			AggrTerm aggrQuery, TemporalQueryTerm tempQuery, String lang, int lastN) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			String sql = "with a as (select id, ('{\"@id\": \"'||e_id||'\", \"" + NGSIConstants.NGSI_LD_CREATED_AT
					+ "\": [{\"@type\": \"" + NGSIConstants.NGSI_LD_DATE_TIME
					+ "\", \"@value\": \"'|| createdat||'\"}], \"" + NGSIConstants.NGSI_LD_MODIFIED_AT
					+ "\": [{\"@type\": \"" + NGSIConstants.NGSI_LD_DATE_TIME
					+ "\", \"@value\": \"'|| modifiedat||'\"}]}')::jsonb as entity from temporalentity where e_id=$1),\n"
					+ "b as (select '@type' as key, jsonb_agg(e_type) as value from a left join tempetype2iid on a.id = tempetype2iid.iid group by a.entity),\n";
			ArrayList<Object> tupleInput = new ArrayList<>();
			tupleInput.add(entityId);
			int dollarCount = 2;
			if (aggrQuery != null) {
				
			} else {
				sql += "c as (select attributeId as key, jsonb_agg(data) as value from a left join temporalentityattrinstance on a.id = temporalentityattrinstance.iid";
				if (attrsQuery != null || tempQuery != null) {
					sql += "where ";
					if (attrsQuery != null) {
						sql += "attributeId in ($" + dollarCount + ") ";
						dollarCount++;
						tupleInput.add(attrsQuery.getAttrs());
						if (tempQuery != null) {
							sql += " AND ";
						}
					}
					if (tempQuery != null) {
						sql += " and $" + dollarCount;
						tupleInput.add(tempQuery.getTimeProperty());
						dollarCount++;
						switch (tempQuery.getTimerel()) {
						case NGSIConstants.TIME_REL_BEFORE:
							sql += " < $" + dollarCount + "::TIMESTAMP ";
							tupleInput.add(tempQuery.getTimeAt());
							dollarCount++;
							break;
						case NGSIConstants.TIME_REL_AFTER:
							sql += " > $" + dollarCount + "::TIMESTAMP ";
							tupleInput.add(tempQuery.getTimeAt());
							dollarCount++;
							break;
						case NGSIConstants.TIME_REL_BETWEEN:
							sql += " between $" + dollarCount + "::TIMESTAMP AND $" + (dollarCount + 1)
									+ "::TIMESTAMP ";
							tupleInput.add(tempQuery.getTimeAt());
							tupleInput.add(tempQuery.getEndTimeAt());
							dollarCount += 2;
							break;
						}
					}
				}
				sql += "group by attributeId limit $" + dollarCount + ")";
			}

			sql += ",\nd as (select jsonb_object_agg(c.key, c.value) as attrs FROM c),\ne as (select '"
					+ NGSIConstants.NGSI_LD_SCOPE
					+ "' as key, jsonb_agg(jsonb_build_object('@id', e_scope)) as value from b, a left join tempescope2iid on a.id = tempescope2iid.iid where e_scope is not null)\n"
					+ "select jsonb_build_object(b.key, b.value, e.key, e.value) || d.attrs || a.entity from a, b, d, e";

			return client.preparedQuery(sql.toString()).execute(Tuple.from(tupleInput)).onFailure().retry().atMost(3)
					.onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));
		});

	}

}