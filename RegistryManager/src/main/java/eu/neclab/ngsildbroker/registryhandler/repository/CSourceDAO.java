package eu.neclab.ngsildbroker.registryhandler.repository;

import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import com.google.common.collect.Lists;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class CSourceDAO {

	@Inject
	ClientManager clientManager;

	@Inject
	MicroServiceUtils microServiceUtils;

	public Uni<RowSet<Row>> getRegistrationById(String tenant, String id) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			switch (id) {

			case AppConstants.INTERNAL_TYPE_REGISTRATION_ID:

				return client
						.preparedQuery("SELECT jsonb_set('{\"https://uri.etsi.org/ngsi-ld/endpoint\": [{\"@value\": \""
								+ microServiceUtils.getGatewayURL().toString() + "\"}],\"@id\": \""
								+ AppConstants.INTERNAL_TYPE_REGISTRATION_ID
								+ "\",\"https://uri.etsi.org/ngsi-ld/information\": [{\"https://uri.etsi.org/ngsi-ld/entities\": [{}]}],\"@type\": [\"https://uri.etsi.org/ngsi-ld/ContextSourceRegistration\"]  }'::jsonb,'{https://uri.etsi.org/ngsi-ld/information,0,https://uri.etsi.org/ngsi-ld/entities,0,@type}' ,jsonb_agg(distinct myTypes)) from entity, jsonb_array_elements(ENTITY -> '@type') as myTypes")
						.execute();
			case AppConstants.INTERNAL_ATTRS_REGISTRATION_ID:
				String sql = """
							SELECT JSONB_SET('{"https://uri.etsi.org/ngsi-ld/endpoint": [{"@value": "http://localhost:9090"}],"@id": "scorpio:hosted:attrs","https://uri.etsi.org/ngsi-ld/information": [{"https://uri.etsi.org/ngsi-ld/entities": [{}]}],"@type": ["https://uri.etsi.org/ngsi-ld/ContextSourceRegistration"]  }'::JSONB,
							'{https://uri.etsi.org/ngsi-ld/information,0,https://uri.etsi.org/ngsi-ld/entities,0}',
							JSONB_BUILD_OBJECT('https://uri.etsi.org/ngsi-ld/propertyNames',
								JSONB_AGG(JSONB_BUILD_OBJECT('@id',	ATTRIBNAME)) FILTER
								(WHERE ENTITY #>> (ATTRIBNAME || '{0,@type,0}'::text[]) = ANY('{https://uri.etsi.org/ngsi-ld/Property, https://uri.etsi.org/ngsi-ld/GeoProperty, https://uri.etsi.org/ngsi-ld/LanguageProperty}')),
								'https://uri.etsi.org/ngsi-ld/relationshipNames',
								JSONB_AGG(JSONB_BUILD_OBJECT('@id',	ATTRIBNAME)) FILTER
								(WHERE ENTITY #>> (ATTRIBNAME || '{0,@type,0}'::text[]) = ANY('{https://uri.etsi.org/ngsi-ld/Relationship}'))))
						FROM ENTITY, Jsonb_object_keys(ENTITY) as attribname
												""";
				System.out.println(sql);
				return client.preparedQuery(sql).execute();
//				break;
//			case AppConstants.INTERNAL_TYPE_ATTRS_REGISTRATION_ID:
//				break;
			case AppConstants.INTERNAL_ID_REGISTRATION_ID:
				return client
						.preparedQuery("SELECT jsonb_set('{\"https://uri.etsi.org/ngsi-ld/endpoint\": [{\"@value\": \""
								+ microServiceUtils.getGatewayURL().toString() + "\"}],\"@id\": \""
								+ AppConstants.INTERNAL_ID_REGISTRATION_ID
								+ "\",\"https://uri.etsi.org/ngsi-ld/information\": [{\"https://uri.etsi.org/ngsi-ld/entities\": [{}]}],\"@type\": [\"https://uri.etsi.org/ngsi-ld/ContextSourceRegistration\"]  }'::jsonb,'{https://uri.etsi.org/ngsi-ld/information,0,https://uri.etsi.org/ngsi-ld/entities}' ,jsonb_agg(jsonb_build_object('@id', id))) from entity")
						.execute();
//			case AppConstants.INTERNAL_FULL_REGISTRATION_ID:
//				break;
			default:
				return client.preparedQuery("SELECT reg FROM csource WHERE c_id = $1").execute(Tuple.of(id)).onFailure()
						.retry().atMost(3);
			}

		});

	}

	public Uni<RowSet<Row>> createRegistration(CreateCSourceRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			return client.preparedQuery("INSERT INTO csource(reg) VALUES ($1)")
					.execute(Tuple.of(new JsonObject(request.getPayload()))).onFailure().retry().atMost(3);
		});
	}

	public Uni<RowSet<Row>> updateRegistration(AppendCSourceRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			return client.preparedQuery("UPDATE csource SET reg=reg || $1 where c_id=$2 RETURNING reg")
					.execute(Tuple.of(new JsonObject(request.getPayload()), request.getId())).onFailure().retry()
					.atMost(3);
		});
	}

	public Uni<RowSet<Row>> deleteRegistration(DeleteCSourceRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			return client.preparedQuery("DELETE FROM csource WHERE c_id=$1 RETURNING reg")
					.execute(Tuple.of(request.getId())).onFailure().retry().atMost(3);
		});
	}

	public Uni<RowSet<Row>> query(String tenant, Set<String> ids, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, CSFQueryTerm csf, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, int limit,
			int offset, boolean count) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			List<Object> tupleItems = Lists.newArrayList();
			String sql = "with a as (select cs_id from contextsourceinformation WHERE ";
			boolean sqlAdded = false;
			int dollar = 1;
			if (ids != null) {
				sql += "(e_id in $" + dollar + " or e_id is null) and (e_id_p is null or any($" + dollar
						+ ") like e_id_p)";
				dollar++;
				tupleItems.add(ids);
				sqlAdded = true;
			}
			if (idPattern != null) {
				if (sqlAdded) {
					sql += " and ";
				}
				sql += "(e_id is null or $" + dollar + " ~ e_id)";
				tupleItems.add(idPattern);
				dollar++;
				sqlAdded = true;
			}
			if (typeQuery != null) {
				if (sqlAdded) {
					sql += " and ";
				}
				tupleItems.add(typeQuery.getAllTypes());
				dollar++;
				sql += "(e_type is null or e_type in $" + dollar + ")";
			}
			if (attrsQuery != null) {
				if (sqlAdded) {
					sql += " and ";
				}
				sql += "(e_prop is null or e_prop in $" + dollar + ") and (e_rel is null or e_rel in $" + dollar + ")";
				tupleItems.add(attrsQuery.getAttrs());
				dollar++;
			}
			if (geoQuery != null) {
				if (sqlAdded) {
					sql += " and ";
				}
				try {
					Tuple2<StringBuilder, Integer> tmp = geoQuery.getGeoSQLQuery(tupleItems, dollar, "i_location");
					sql += tmp.getItem1().toString();
					dollar = tmp.getItem2();
				} catch (ResponseException e) {
					return Uni.createFrom().failure(e);
				}
			}
			if (scopeQuery != null) {
				if (sqlAdded) {
					sql += " and ";
				}
				sql += "(scopes IS NULL OR ";
				ScopeQueryTerm current = scopeQuery;
				while (current != null) {
					sql += " matchscope(scopes, " + current.getSQLScopeQuery() + ")";

					if (current.hasNext()) {
						if (current.isNextAnd()) {
							sql += " and ";
						} else {
							sql += " or ";
						}
					}
					current = current.getNext();
				}
				sql += ")";

			}

			sql += ") select csource.reg from a left join csource on a.cs_id = csource.id";
			if (csf != null) {
				// if (sqlAdded) {
				// sql += " and ";
				// }
				// dollar++;
			}

			return client.preparedQuery(sql).execute(Tuple.from(tupleItems)).onFailure().retry().atMost(3);
		});

	}

	public Uni<List<String>> getAllTenants() {
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery("select tenant_id from tenant").execute().onItem().transform(rows -> {
				List<String> result = Lists.newArrayList();
				rows.forEach(row -> {
					result.add(row.getString(0));
				});
				result.add(AppConstants.INTERNAL_NULL_KEY);
				return result;
			});
		});
	}

}
