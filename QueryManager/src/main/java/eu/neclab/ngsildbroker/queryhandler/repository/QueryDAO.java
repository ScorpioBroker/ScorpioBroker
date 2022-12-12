package eu.neclab.ngsildbroker.queryhandler.repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Singleton;

import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.EntityStorageFunctions;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class QueryDAO extends StorageDAO {

	public Uni<Map<String, Object>> getEntity(String entryId, String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT ENTITY FROM ENTITY WHERE E_ID=$1").execute(Tuple.of(entryId)).onItem()
					.transformToUni(t -> {
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
					"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode, (array_agg(DISTINCT C.e_prop) FILTER (WHERE C.e_prop is not null) || array_agg(DISTINCT C.e_rel) FILTER (WHERE C.e_rel is not null)) AS attrs FROM CSOURCEINFORMATION AS C WHERE C.retrieveEntity=true AND (C.E_ID=$1 OR C.E_ID=NULL) AND (C.e_prop=NULL OR C.e_prop IN $2) AND (C.e_rel=NULL OR C.e_rel IN $2) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') GROUP BY C.endpoint, C.tenant_id, c.headers, c.reg_mode")
					.execute(Tuple.of(entityId, expandedAttrs));
		});
	}

	public Uni<RowSet<Row>> query(String tenantId, Set<String> ids, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, int limit,
			int offSet, boolean count) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			StringBuilder query = new StringBuilder("WITH ");
			char currentChar = 'a';
			boolean sqlAdded = false;
			Tuple2<Character, String> tmp;
			if (typeQuery != null) {
				tmp = typeQuery.toSql(currentChar);
				currentChar = tmp.getItem1();
				query.append(tmp.getItem2());
				sqlAdded = true;
			}
			if (attrsQuery != null) {
				if (sqlAdded) {
					query.append("),");
					try {
						tmp = attrsQuery.toSql((char) (currentChar + 1), currentChar);
					} catch (ResponseException e) {
						return Uni.createFrom().failure(e);
					}
				} else {
					try {
						tmp = attrsQuery.toSql((char) (currentChar + 1), null);
					} catch (ResponseException e) {
						return Uni.createFrom().failure(e);
					}
				}

				currentChar = tmp.getItem1();
				query.append(tmp.getItem2());
				sqlAdded = true;
			}
			if (geoQuery != null) {
				if (sqlAdded) {
					query.append("),");
					try {
						tmp = geoQuery.toSql((char) (currentChar + 1), currentChar);
					} catch (ResponseException e) {
						return Uni.createFrom().failure(e);
					}
				} else {
					try {
						tmp = geoQuery.toSql((char) (currentChar + 1), null);
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
						tmp = qQuery.toSql((char) (currentChar + 1), currentChar);
					} catch (ResponseException e) {
						return Uni.createFrom().failure(e);
					}
				} else {
					try {
						tmp = qQuery.toSql((char) (currentChar + 1), null);
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
					query.append(".iid) as count, null as entity FROM ");
					query.append(currentChar);
					query.append(';');
				} else {
					query.append(" LIMIT ");
					query.append(limit);
					query.append(" OFFSET ");
					query.append(offSet);
					query.append("), SELECT count(");
					query.append(currentChar);
					query.append(".iid) as count, entity.entity as entity FROM ");
					query.append(currentChar);
					query.append(" LEFT JOIN ENTITY ON ");
					query.append(currentChar);
					query.append(".iid = ENTITY.ID;");
				}
			} else {
				query.append("), SELECT count(entity.id) as count, entity.entity as entity FROM ");
				query.append(currentChar);
				query.append(" LEFT JOIN ENTITY ON ");
				query.append(currentChar);
				query.append(".iid = ENTITY.ID WHERE ");
				if (ids != null) {
					for (String id : ids) {
						query.append("ENTITY.E_ID = '");
						query.append(id);
						query.append("' or ");
					}
					query.setLength(query.length() - 4);
				}
				if (idPattern != null) {
					query.append(idPattern);
					query.append(" ~ ENTITY.E_ID");
				}
				query.append(" LIMIT ");
				query.append(limit);
				query.append(" OFFSET ");
				query.append(offSet);
				query.append(';');
			}
			// TODO at the moment this does no sql escaping toSql method should return tuple
			// with respective values
			return client.preparedQuery(query.toString()).execute();
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

	public Uni<RowSet<Row>> getRemoteSourcesForQuery(String tenantId, Set<String> id, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, CSFQueryTerm csf, GeoQueryTerm geoQuery,
			ScopeQueryTerm scopeQuery) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			Set<String> attribs = Sets.newHashSet();
			if(attrsQuery != null) {
				attribs.addAll(attrsQuery.getAttrs());
			}
			if()
			"SELECT C.endpoint C.tenant_id, c.headers, c.reg_mode, c.queryEntity, c.queryBatch FROM CSOURCEINFORMATION AS C WHERE (c.queryEntity OR c.queryBatch) AND (C.e_prop=NULL OR C.e_prop=$1) AND (C.e_rel=NULL OR C.e_rel=$1)"
		});
		
	}

}
