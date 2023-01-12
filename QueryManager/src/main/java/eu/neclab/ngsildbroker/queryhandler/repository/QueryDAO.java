package eu.neclab.ngsildbroker.queryhandler.repository;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple4;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class QueryDAO {

	@Inject
	ClientManager clientManager;

	public Uni<Map<String, Object>> getEntity(String entryId, String tenantId, AttrsQueryTerm attrsQuery) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			String sql = "";
			sql += "SELECT ";
			int dollar = 1;
			List<Object> tupleItems = Lists.newArrayList();
			if (attrsQuery != null) {
				sql += "jsonb_strip_nulls(jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "', ENTTIY->"
						+ NGSIConstants.JSON_LD_ID + ", '" + NGSIConstants.JSON_LD_TYPE + "', ENTTIY->"
						+ NGSIConstants.JSON_LD_TYPE + ", '" + NGSIConstants.NGSI_LD_CREATED_AT + "', ENTTIY->"
						+ NGSIConstants.NGSI_LD_CREATED_AT + ", '" + NGSIConstants.NGSI_LD_MODIFIED_AT + "', ENTTIY->"
						+ NGSIConstants.NGSI_LD_MODIFIED_AT + ", ";
				Iterator<String> it = attrsQuery.getAttrs().iterator();
				while (it.hasNext()) {
					sql += "'$" + dollar + "', ENTITY->$" + dollar;
					dollar++;
					tupleItems.add(it.next());
					if (it.hasNext()) {
						sql += ", ";
					}
				}

				sql += ")) AS ENTITY";
			} else {
				sql += "ENTITY";
			}
			sql += " FROM ENTITY WHERE E_ID=$" + dollar;
			tupleItems.add(entryId);
			return client.preparedQuery(sql).execute(Tuple.from(tupleItems)).onItem().transformToUni(t -> {
				if (t.rowCount() == 0) {
					return Uni.createFrom().item(new HashMap<String, Object>());
				}
				return Uni.createFrom().item(t.iterator().next().getJsonObject(0).getMap());
			});
		});

	}

	public Uni<RowSet<Row>> getRemoteSourcesForEntity(String entityId, Set<String> expandedAttrs, String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode, (array_agg(DISTINCT C.e_prop) FILTER (WHERE C.e_prop is not null) || array_agg(DISTINCT C.e_rel) FILTER (WHERE C.e_rel is not null)) AS attrs FROM CSOURCEINFORMATION AS C WHERE C.retrieveEntity AND (C.E_ID=$1 OR C.E_ID=NULL) AND (C.e_prop=NULL OR C.e_prop IN $2) AND (C.e_rel=NULL OR C.e_rel IN $2) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') GROUP BY C.endpoint, C.tenant_id, c.headers, c.reg_mode")
					.execute(Tuple.of(entityId, expandedAttrs));
		});
	}

	public Uni<RowSet<Row>> queryLocalOnly(String tenantId, Set<String> ids, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, LanguageQueryTerm langQuery, int limit,
			int offSet, boolean count) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			StringBuilder query = new StringBuilder("WITH ");
			char currentChar = 'a';
			boolean sqlAdded = false;
			int dollar = 1;
			List<Object> tupleItems = Lists.newArrayList();
			Tuple4<Character, String, Integer, List<Object>> tmp;
			if (typeQuery != null) {
				tmp = typeQuery.toSql(currentChar, dollar);
				currentChar = tmp.getItem1();
				query.append(tmp.getItem2());
				dollar = tmp.getItem3();
				tupleItems.addAll(tmp.getItem4());
				sqlAdded = true;
			}
			if (attrsQuery != null) {
				if (sqlAdded) {
					query.append("),");
					try {
						tmp = attrsQuery.toSql((char) (currentChar + 1), currentChar, dollar);
					} catch (ResponseException e) {
						return Uni.createFrom().failure(e);
					}
				} else {
					try {
						tmp = attrsQuery.toSql((char) (currentChar + 1), null, dollar);
					} catch (ResponseException e) {
						return Uni.createFrom().failure(e);
					}
				}

				currentChar = tmp.getItem1();
				query.append(tmp.getItem2());
				dollar = tmp.getItem3();
				tupleItems.addAll(tmp.getItem4());
				sqlAdded = true;
			}
			if (geoQuery != null) {
				if (sqlAdded) {
					query.append("),");
					try {
						tmp = geoQuery.toSql((char) (currentChar + 1), currentChar, dollar);
					} catch (ResponseException e) {
						return Uni.createFrom().failure(e);
					}
				} else {
					try {
						tmp = geoQuery.toSql((char) (currentChar + 1), null, dollar);
					} catch (ResponseException e) {
						return Uni.createFrom().failure(e);
					}
				}
				currentChar = tmp.getItem1();
				query.append(tmp.getItem2());
				sqlAdded = true;
			}
			if (qQuery != null) {
				if (sqlAdded) {
					query.append("),");
					try {
						tmp = qQuery.toSql((char) (currentChar + 1), currentChar, dollar);
					} catch (ResponseException e) {
						return Uni.createFrom().failure(e);
					}
				} else {
					try {
						tmp = qQuery.toSql((char) (currentChar + 1), null, dollar);
					} catch (ResponseException e) {
						return Uni.createFrom().failure(e);
					}
				}
				currentChar = tmp.getItem1();
				query.append(tmp.getItem2());
				sqlAdded = true;
			}
			if (ids == null && idPattern == null) {
				if (limit == 0 && count) {
					query.append("), SELECT count(");
					query.append(currentChar);
					query.append(".iid) as count FROM ");
					query.append(currentChar);
					query.append(';');
				} else {
					query.append("), entityCount as (SELECT count(");
					query.append(currentChar);
					query.append(".iid) as count FROM ");
					query.append(currentChar);
					query.append("), ");
					query.append((char) (currentChar + 1));
					query.append(" as (SELECT ");
					query.append(currentChar);
					query.append(".iid FROM ");
					query.append(currentChar);
					query.append(" LIMIT ");
					query.append(limit);
					query.append(" OFFSET ");
					query.append(offSet);
					currentChar++;
					query.append("), SELECT entityCount.count As count, entity.entity as entity FROM entityCount, ");
					query.append(currentChar);
					query.append(" LEFT JOIN ENTITY ON ");
					query.append(currentChar);
					query.append(".iid = ENTITY.ID;");
				}
			} else {
				query.append("), SELECT count(entity.id) as count");
				if (limit != 0 && count) {
					query.append(", entity.entity as entity");
				}
				query.append(" FROM ");
				query.append(currentChar);
				query.append(" LEFT JOIN ENTITY ON ");
				query.append(currentChar);
				query.append(".iid = ENTITY.ID WHERE ");
				if (ids != null) {
					for (String id : ids) {
						query.append("ENTITY.E_ID = '$");
						query.append(dollar);
						dollar++;
						tupleItems.add(id);
						query.append("' or ");
					}
					query.setLength(query.length() - 4);
				}
				if (idPattern != null) {
					query.append("$");
					query.append(dollar);
					tupleItems.add(idPattern);
					query.append(" ~ ENTITY.E_ID");
				}
				query.append(" LIMIT ");
				query.append(limit);
				query.append(" OFFSET ");
				query.append(offSet);
				query.append(';');
			} //
			return client.preparedQuery(query.toString()).execute(Tuple.from(tupleItems));
		});
	}

	public Uni<RowSet<Row>> getTypes(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT DISTINCT e_type FROM etype2iid").execute();
		});
	}

	public Uni<RowSet<Row>> getTypesWithDetails(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"WITH T as (SELECT e_type, iid FROM etype2iid) SELECT DISTINCT T.e_type, A.attr FROM T LEFT JOIN attr2iid as A ON A.iid=T.iid")
					.execute();
		});
	}

	public Uni<RowSet<Row>> getType(String tenantId, String type) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					// needs type, entitycount for type, attribs with entitycount attributetype
					// (isRel, isGeo),
					// typeNames of types where the attrib is used this is stupidly expensive for
					// this and is optional in the spec i tend to not have it
					"WITH T as (SELECT e_type, iid FROM etype2iid WHERE e_type = $1) SELECT DISTINCT T.e_type, A.attr FROM T LEFT JOIN attr2iid as A ON A.iid=T.iid")
					.execute(Tuple.of(type));
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForTypes(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveEntityTypes=true")
					.execute();
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForTypesWithDetails(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveEntityTypeDetails=true")
					.execute();
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForType(String tenantId, String type) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveEntityTypeInfo=true AND (C.e_type=NULL OR C.e_type=$1)")
					.execute(Tuple.of(type));
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForAttribs(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveAttrTypes=true")
					.execute();
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForAttribsWithDetails(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveAttrTypeDetails=true")
					.execute();
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForAttrib(String tenantId, String attrib) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveAttrTypeInfo=true AND (C.e_prop=NULL OR C.e_prop=$1) AND (C.e_rel=NULL OR C.e_rel=$1)")
					.execute(Tuple.of(attrib));
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForQuery(String tenantId, Set<String> id, TypeQueryTerm typeQuery,
			String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery, CSFQueryTerm csf, GeoQueryTerm geoQuery,
			ScopeQueryTerm scopeQuery) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			Set<String> attribs = Sets.newHashSet();
			if (attrsQuery != null) {
				attribs.addAll(attrsQuery.getAttrs());
			}
			if (qQuery != null && attrsQuery == null && typeQuery == null) {
				Set<String> result = Sets.newHashSet();
				getAttribsFromQuery(qQuery, result);
				attribs.addAll(result);
			}
			Set<String> types = Sets.newHashSet();
			if (typeQuery != null) {
				getTypesFromQuery(typeQuery, types);
			}
			List<Object> prepareObjects = Lists.newArrayList();
			StringBuilder queryFront = new StringBuilder(
					"SELECT C.endpoint C.tenant_id, c.headers, c.reg_mode, c.queryEntity, c.queryBatch");
			StringBuilder wherePart = new StringBuilder(" WHERE ");
			int dollarCount = 1;
			if (!types.isEmpty()) {
				queryFront.append(", array_agg(DISTINCT C.e_type) FILTER (WHERE C.e_type is not null) as entityType");
				wherePart.append("(C.e_type is NULL OR C.e_type in $");
				wherePart.append(dollarCount);
				wherePart.append(") AND ");
				prepareObjects.add(types);
				dollarCount++;
			} else {
				queryFront.append(", null as entityType");
			}
			if (id != null) {
				queryFront.append(", array_agg(DISTINCT C.e_id) FILTER (WHERE C.e_id is not null) as entityId");
				wherePart.append("(C.e_id is NULL OR C.e_id in $");
				wherePart.append(dollarCount);
				wherePart.append(") AND (C.e_id_p is NULL OR C.e_id_p ~ ANY($");
				wherePart.append(dollarCount);
				wherePart.append(")) AND ");
				prepareObjects.add(id);
				dollarCount++;
			} else {
				queryFront.append(", null as entityId");
			}
			if (idPattern != null) {
				queryFront.append(", array_agg(DISTINCT C.e_id) FILTER (WHERE C.e_id is not null) as entityId");
				wherePart.append("(C.e_id is NULL OR $");
				wherePart.append(dollarCount);
				wherePart.append(" ~ C.e_id) AND (C.e_id_p is NULL OR $");
				wherePart.append(dollarCount);
				wherePart.append(" ~ C.e_id_p) AND ");
				prepareObjects.add(id);
				dollarCount++;
			} else {
				queryFront.append(", null as entityId");
			}
			if (attribs.isEmpty()) {
				queryFront.append(
						", (array_agg(DISTINCT C.e_prop) FILTER (WHERE C.e_prop is not null) || array_agg(DISTINCT C.e_rel) FILTER (WHERE C.e_rel is not null)) AS attrs");
				wherePart.append("(C.e_prop=NULL OR C.e_prop IN $");
				wherePart.append(dollarCount);
				wherePart.append(") AND (C.e_rel=NULL OR C.e_rel IN $");
				wherePart.append(dollarCount);
				wherePart.append(")) AND ");
			} else {
				queryFront.append(", null as attrs");
			}
			if (geoQuery != null) {
				// TODO user intersect between search area and registration area
			} else {
				queryFront.append(", null as geoq");
			}
			if (scopeQuery != null) {
				// TODO array check between scopes from query and reg query
			} else {
				queryFront.append(", null as scopeq");
			}
			if (csf != null) {
				// TODO talk with martin to understand csf better
			}
			// remove last " AND "
			wherePart.setLength(wherePart.length() - 5);
			queryFront.append(" FROM CSOURCEINFORMATION AS C");
			queryFront.append(wherePart);

			return client.preparedQuery(queryFront.toString()).execute(Tuple.from(prepareObjects));
		});

	}

	private void getTypesFromQuery(TypeQueryTerm typeQuery, Set<String> result) {
		String type = typeQuery.getType();
		if (type != null) {
			result.add(type);
		}
		TypeQueryTerm child = typeQuery.getFirstChild();
		if (child != null) {
			getTypesFromQuery(child, result);
		}

		while (typeQuery.hasNext()) {
			getTypesFromQuery(typeQuery.getNext(), result);
		}
	}

	private void getAttribsFromQuery(QQueryTerm qQuery, Set<String> result) {
		String attrib = qQuery.getAttribute();
		if (attrib != null) {
			result.add(attrib);
		}
		QQueryTerm child = qQuery.getFirstChild();
		if (child != null) {
			getAttribsFromQuery(child, result);
		}

		while (qQuery.hasNext()) {
			getAttribsFromQuery(qQuery.getNext(), result);
		}
	}

}
