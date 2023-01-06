package eu.neclab.ngsildbroker.registryhandler.repository;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.collect.Lists;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
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
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class CSourceDAO {

	@Inject
	ClientManager clientManager;

	public Uni<RowSet<Row>> getRegistrationById(String tenant, String id) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			switch (id) {

//			case AppConstants.INTERNAL_TYPE_REGISTRATION_ID:
//				break;
//			case AppConstants.INTERNAL_ATTRS_REGISTRATION_ID:
//				break;
//			case AppConstants.INTERNAL_TYPE_ATTRS_REGISTRATION_ID:
//				break;
//			case AppConstants.INTERNAL_ID_REGISTRATION_ID:
//				break;
//			case AppConstants.INTERNAL_FULL_REGISTRATION_ID:
//				break;
			default:
				return client.preparedQuery("SELECT reg FROM csource WHERE id = $1").execute(Tuple.of(id)).onFailure()
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

	public Uni<List<Tuple2<RemoteHost, Map<String, Object>>>> getLocalRegistrationId(List<Tuple3<String, String, String>> tenant2RegType2TargetTenant, String defaultRegType) {
//		if(tenant2RegType2TargetTenant == null) {
//			clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
//				return client.preparedQuery("SELECT tenant_id FROM tenant").execute().onItem().transformToUni(rows -> {
//					List<Uni<Tuple2<RemoteHost, Map<String, Object>>>> unis = Lists.newArrayList();
//					rows.forEach(row -> {
//						unis.add(getRegistrationById(row.getString(0), defaultRegType).onItem().transform(null)
//								)
//					});
//					return Uni.combine().all().unis(unis).combinedWith(list -> null);
//				});
//			});
//			for(String tenant: clientManager.getAllClients().keySet()) {
//				
//			}
//		}
//		clientManager.getClient(null, false).onItem().transform(clie)
		
		return null;
	}

}
