package eu.neclab.ngsildbroker.registryhandler.repository;

import com.google.common.collect.Lists;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
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
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Set;

@Singleton
public class CSourceDAO {

	//private final static Logger logger = LoggerFactory.getLogger(CSourceDAO.class);
	@Inject
	ClientManager clientManager;

	@Inject
	MicroServiceUtils microServiceUtils;

	public Uni<RowSet<Row>> getRegistrationById(String tenant, String id) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			switch (id) {
			case AppConstants.INTERNAL_TYPE_REGISTRATION_ID -> {
				String sql1 = "SELECT jsonb_set('{\"https://uri.etsi.org/ngsi-ld/endpoint\": [{\"@value\": \""
						+ microServiceUtils.getGatewayURL().toString() + "\"}],\"@id\": \""
						+ AppConstants.INTERNAL_TYPE_REGISTRATION_ID
						+ "\",\"https://uri.etsi.org/ngsi-ld/information\": [{\"https://uri.etsi.org/ngsi-ld/entities\": [{}]}],\"@type\": [\"https://uri.etsi.org/ngsi-ld/ContextSourceRegistration\"]  }'::jsonb,'{https://uri.etsi.org/ngsi-ld/information,0,https://uri.etsi.org/ngsi-ld/entities,0,@type}' ,jsonb_agg(distinct myTypes)) from entity, jsonb_array_elements(ENTITY -> '@type') as myTypes";

				return client.preparedQuery(sql1).execute();
			}
			case AppConstants.INTERNAL_ATTRS_REGISTRATION_ID -> {
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

				return client.preparedQuery(sql).execute();
			}
			case AppConstants.INTERNAL_TYPE_ATTRS_REGISTRATION_ID -> {
				return client.preparedQuery(
						"""
								SELECT JSONB_SET('{"https://uri.etsi.org/ngsi-ld/endpoint": [{"@value": "%s"}],"@id": "%s","https://uri.etsi.org/ngsi-ld/information": [{"https://uri.etsi.org/ngsi-ld/entities": [{}]}],"@type": ["https://uri.etsi.org/ngsi-ld/ContextSourceRegistration"]  }'::JSONB,
								                            '{https://uri.etsi.org/ngsi-ld/information,0,https://uri.etsi.org/ngsi-ld/entities,0}',
								                            JSONB_BUILD_OBJECT('https://uri.etsi.org/ngsi-ld/propertyNames',
								                            JSONB_AGG(JSONB_BUILD_OBJECT('@id',ATTRIBNAME)) FILTER
								                            (WHERE ENTITY #>> (ATTRIBNAME || '{0,@type,0}'::text[]) = ANY('{https://uri.etsi.org/ngsi-ld/Property, https://uri.etsi.org/ngsi-ld/GeoProperty, https://uri.etsi.org/ngsi-ld/LanguageProperty}')),
								                            'types',
								                            (SELECT jsonb_agg(distinct myTypes) from entity, jsonb_array_elements(ENTITY -> '@type') as myTypes ),
								                            'https://uri.etsi.org/ngsi-ld/relationshipNames',
								                            JSONB_AGG(JSONB_BUILD_OBJECT('@id',ATTRIBNAME)) FILTER
								                            (WHERE ENTITY #>> (ATTRIBNAME || '{0,@type,0}'::text[]) = ANY('{https://uri.etsi.org/ngsi-ld/Relationship}'))))
								                            FROM ENTITY, Jsonb_object_keys(ENTITY) as attribname"""
								.formatted(microServiceUtils.getGatewayURL().toString(),
										AppConstants.INTERNAL_TYPE_ATTRS_REGISTRATION_ID))
						.execute();
			}
			case AppConstants.INTERNAL_ID_REGISTRATION_ID -> {
				return client
						.preparedQuery("SELECT jsonb_set('{\"https://uri.etsi.org/ngsi-ld/endpoint\": [{\"@value\": \""
								+ microServiceUtils.getGatewayURL().toString() + "\"}],\"@id\": \""
								+ AppConstants.INTERNAL_ID_REGISTRATION_ID
								+ "\",\"https://uri.etsi.org/ngsi-ld/information\": [{\"https://uri.etsi.org/ngsi-ld/entities\": [{}]}],\"@type\": [\"https://uri.etsi.org/ngsi-ld/ContextSourceRegistration\"]  }'::jsonb,'{https://uri.etsi.org/ngsi-ld/information,0,https://uri.etsi.org/ngsi-ld/entities}' ,jsonb_agg(jsonb_build_object('@id', id))) from entity")
						.execute();
			}
			case AppConstants.INTERNAL_FULL_REGISTRATION_ID -> {
				return client.preparedQuery(
						"""
								SELECT\s
								  JSONB_SET(
								    '{"https://uri.etsi.org/ngsi-ld/endpoint": [{"@value": "%s"}],"@id": "%s","https://uri.etsi.org/ngsi-ld/information": [{"https://uri.etsi.org/ngsi-ld/entities": [{}]}],"@type": ["https://uri.etsi.org/ngsi-ld/ContextSourceRegistration"]  }' :: JSONB,\s
								    '{https://uri.etsi.org/ngsi-ld/information,0,https://uri.etsi.org/ngsi-ld/entities,0}',\s
								    JSONB_BUILD_OBJECT(
								\t\t'ids',
								\t\t(select jsonb_agg( id) from entity),
								      'https://uri.etsi.org/ngsi-ld/propertyNames',\s
								      JSONB_AGG(
								        JSONB_BUILD_OBJECT('@id', ATTRIBNAME)
								      ) FILTER (
								        WHERE\s
								          ENTITY #>> (ATTRIBNAME || '{0,@type,0}'::text[]) = ANY('{https://uri.etsi.org/ngsi-ld/Property, https://uri.etsi.org/ngsi-ld/GeoProperty, https://uri.etsi.org/ngsi-ld/LanguageProperty}')),
								          'types',\s
								          (
								            SELECT\s
								              jsonb_agg(distinct myTypes)\s
								            from\s
								              entity,\s
								              jsonb_array_elements(ENTITY -> '@type') as myTypes
								          ),\s
								          'https://uri.etsi.org/ngsi-ld/relationshipNames',\s
								          JSONB_AGG(
								            JSONB_BUILD_OBJECT('@id', ATTRIBNAME)
								          ) FILTER (
								            WHERE\s
								              ENTITY #>> (ATTRIBNAME || '{0,@type,0}'::text[]) = ANY('{https://uri.etsi.org/ngsi-ld/Relationship}'))))
								            FROM\s
								              ENTITY,\s
								              Jsonb_object_keys(ENTITY) as attribname
								"""
								.formatted(microServiceUtils.getGatewayURL().toString(),
										AppConstants.INTERNAL_FULL_REGISTRATION_ID))
						.execute();
			}
			default -> {
				return client.preparedQuery("SELECT reg FROM csource WHERE c_id = $1").execute(Tuple.of(id)).onFailure()
						.retry().atMost(3);
			}
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
		return clientManager.getClient(request.getTenant(), true).onItem()
				.transformToUni(client -> client.preparedQuery("DELETE FROM csource WHERE c_id=$1 RETURNING reg")
						.execute(Tuple.of(request.getId())).onFailure().retry().atMost(3));
	}

	public Uni<RowSet<Row>> query(String tenant, Set<String> ids, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, CSFQueryTerm csf, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery,
			QQueryTerm qQueryTerm, int limit, int offset, boolean count) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			StringBuilder sql = new StringBuilder(
					"with a as (select distinct cs_id from csourceinformation, csource WHERE ");
			boolean sqlAdded = false;
			int dollar = 1;
			Tuple tuple = Tuple.tuple();
			if (ids != null) {
				sql.append("(e_id =any($");
				sql.append(dollar);
				sql.append(") or e_id is null) and (e_id_p is null or e_id_p like any($");
				sql.append(dollar);
				sql.append("))");
				tuple.addArrayOfString(ids.toArray(new String[0]));
				dollar++;
				sqlAdded = true;
			}
			if (idPattern != null) {
				if (sqlAdded) {
					sql.append(" and ");
				}
				sql.append("(e_id is null or $");
				sql.append(dollar);
				sql.append(" ~ e_id)");
				tuple.addString(idPattern);
				dollar++;
				sqlAdded = true;
			}
			if (qQueryTerm != null) {
				if (sqlAdded) {
					sql.append(" AND ");
				}
				StringBuilder tempSql = new StringBuilder();
				dollar = qQueryTerm.toSql(tempSql, dollar, tuple, true, false);
				sql.append(tempSql.toString().toLowerCase().replace("entity", "csource.reg"));
				sqlAdded = true;
			}
			if (typeQuery != null) {
				if (sqlAdded) {
					sql.append(" and ");
				}
				sql.append("(e_type is null or e_type =any($");
				sql.append(dollar);
				dollar++;
				tuple.addArrayOfString(typeQuery.getAllTypes().toArray(new String[0]));
				sql.append("))");
				sqlAdded = true;
			}
			if (attrsQuery != null) {
				if (sqlAdded) {
					sql.append(" and ");
				}
				sql.append("(e_prop is null or e_prop =any($");
				sql.append(dollar);
				sql.append(")) and (e_rel is null or e_rel =any($");
				sql.append(dollar);
				sql.append("))");
				tuple.addArrayOfString(attrsQuery.getAttrs().toArray(new String[0]));
				dollar++;
			}
			if (geoQuery != null) {
				if (sqlAdded) {
					sql.append(" and ");
				}
				try {
					Tuple2<StringBuilder, Integer> tmp = geoQuery.getGeoSQLQuery(null, dollar, "i_location");
					sql.append(tmp.getItem1().toString());
					dollar = tmp.getItem2();
				} catch (ResponseException e) {
					return Uni.createFrom().failure(e);
				}
			}
			if (scopeQuery != null) {
				if (sqlAdded) {
					sql.append(" and ");
				}
				sql.append("(scopes IS NULL OR ");
				ScopeQueryTerm current = scopeQuery;
				while (current != null) {
					sql.append(" matchscope(scopes, ");
					sql.append(current.getSQLScopeQuery());
					sql.append(')');

					if (current.hasNext()) {
						if (current.isNextAnd()) {
							sql.append(" and ");
						} else {
							sql.append(" or ");
						}
					}
					current = current.getNext();
				}
				sql.append(')');

			}

			sql.append(
					") select csource.reg , (SELECT COUNT(*) FROM a) from a left join csource on a.cs_id = csource.id "
							+ "group by csource.reg " + "limit $");
			sql.append(dollar++);
			tuple.addInteger(limit);
			sql.append(" offset $");
			sql.append(dollar++);
			tuple.addInteger(offset);
			if (csf != null) {
				// if (sqlAdded) {
				// sql += " and ";
				// }
				// dollar++;
			}
			//String sqlString = sql.toString();
			//logger.debug("SQL: " + sqlString);
			//logger.debug("Tuple: " + tuple.deepToString());
			return client.preparedQuery(sql.toString()).execute(tuple).onFailure().retry().atMost(3);
		});

	}

	public Uni<List<String>> getAllTenants() {
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(
				client -> client.preparedQuery("select tenant_id from tenant").execute().onItem().transform(rows -> {
					List<String> result = Lists.newArrayList();
					rows.forEach(row -> {
						result.add(row.getString(0));
					});
					result.add(AppConstants.INTERNAL_NULL_KEY);
					return result;
				}));
	}

	public Uni<Boolean> isTenantPresent(String tenantName) {
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem()
				.transformToUni(client -> client.preparedQuery("select tenant_id from tenant where tenant_id=$1")
						.execute(Tuple.of(tenantName)).onItem().transform(rows -> rows.size() > 0));
	}

}
