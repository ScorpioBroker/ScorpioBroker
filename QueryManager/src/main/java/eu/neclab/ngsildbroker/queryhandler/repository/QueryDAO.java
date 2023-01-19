package eu.neclab.ngsildbroker.queryhandler.repository;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.io.GeoJSONReader;
import org.locationtech.spatial4j.shape.Shape;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple4;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class QueryDAO {

	@Inject
	ClientManager clientManager;

	GeoJSONReader geoReader = new GeoJSONReader(JtsSpatialContext.GEO, new SpatialContextFactory());

	private static Logger logger = LoggerFactory.getLogger(QueryDAO.class);

	public Uni<Map<String, Object>> getEntity(String entityId, String tenantId, AttrsQueryTerm attrsQuery) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			String sql = "";
			sql += "SELECT ENTITY";

			Tuple tuple = Tuple.tuple();
			if (attrsQuery != null) {
				sql += "-$1 FROM ENTITY WHERE ID=$2";
				tuple.addArrayOfString(attrsQuery.getAttrs().toArray(new String[0]));
			} else {
				sql += " FROM ENTITY WHERE ID=$1";
			}
			tuple.addString(entityId);
			return client.preparedQuery(sql).execute(tuple).onItem().transformToUni(t -> {
				if (t.rowCount() == 0) {
					return Uni.createFrom().item(new HashMap<String, Object>());
				}
				return Uni.createFrom().item(t.iterator().next().getJsonObject(0).getMap());
			});
		});

	}

	public Uni<RowSet<Row>> getRemoteSourcesForEntity(String entityId, AttrsQueryTerm attrs, String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {

			if (attrs == null) {
				return client.preparedQuery(
						"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode, (array_agg(DISTINCT C.e_prop) FILTER (WHERE C.e_prop is not null) || array_agg(DISTINCT C.e_rel) FILTER (WHERE C.e_rel is not null)) AS attrs FROM CSOURCEINFORMATION AS C WHERE C.retrieveEntity AND (C.E_ID=$1 OR C.E_ID is NULL)  AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') GROUP BY C.endpoint, C.tenant_id, c.headers, c.reg_mode")
						.execute(Tuple.of(entityId));
			} else {
				return client.preparedQuery(
						"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode, (array_agg(DISTINCT C.e_prop) FILTER (WHERE C.e_prop is not null) || array_agg(DISTINCT C.e_rel) FILTER (WHERE C.e_rel is not null)) AS attrs FROM CSOURCEINFORMATION AS C WHERE C.retrieveEntity AND (C.E_ID=$1 OR C.E_ID is NULL) AND (C.e_prop is NULL OR C.e_prop IN $2) AND (C.e_rel is NULL OR C.e_rel IN $2) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') GROUP BY C.endpoint, C.tenant_id, c.headers, c.reg_mode")
						.execute(Tuple.of(entityId, attrs.getAttrs()));
			}

		});
	}

	public Uni<RowSet<Row>> queryLocalOnly(String tenantId, Set<String> ids, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery,
			LanguageQueryTerm langQuery, int limit, int offSet, boolean count) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			StringBuilder query = new StringBuilder();
			int dollar = 1;
			Tuple tuple = Tuple.tuple();
			if (count && limit == 0) {
				query.append("SELECT COUNT(ENTITY) ");
			} else if (count) {
				query.append("SELECT ENTITY");
				if (attrsQuery != null) {
					query.append("-$");
					query.append(dollar);
					dollar++;
					tuple.addArrayOfString(attrsQuery.getAttrs().toArray(new String[0]));
				}
				query.append(", COUNT(*)");
			} else {
				query.append("SELECT ENTITY");
				if (attrsQuery != null) {
					query.append("-$");
					query.append(dollar);
					dollar++;
					tuple.addArrayOfString(attrsQuery.getAttrs().toArray(new String[0]));
				}
			}
			query.append(" FROM ENTITY WHERE ");
			boolean sqlAdded = false;
			if (typeQuery != null) {
				dollar = typeQuery.toSql(query, tuple, dollar);
				sqlAdded = true;
			}
			if (attrsQuery != null) {
				if (sqlAdded) {
					query.append("AND ");
				}
				dollar = attrsQuery.toSql(query, tuple, dollar);
				sqlAdded = true;
			}
			if (geoQuery != null) {
				if (sqlAdded) {
					query.append("AND ");
				}
				dollar = geoQuery.toSql(query, tuple, dollar);
				sqlAdded = true;
			}

			if (qQuery != null) {
				if (sqlAdded) {
					query.append("AND ");
				}
				query.append(qQuery.toSql());
				sqlAdded = true;
			}
			if (ids != null) {
				if (sqlAdded) {
					query.append("AND ");
				}
				query.append("id IN (");
				for (String id : ids) {
					query.append('$');
					query.append(dollar);
					query.append(',');
					tuple.addString(id);
					dollar++;
				}

				query.setCharAt(query.length() - 1, ')');
				sqlAdded = true;
			}
			if (idPattern != null) {
				if (sqlAdded) {
					query.append("AND ");
				}
				query.append("id ~ $");
				query.append(dollar);
				dollar++;
				sqlAdded = true;
			}

			query.append(" LIMIT ");
			query.append(limit);
			query.append(" OFFSET ");
			query.append(offSet);
			query.append(';');
			System.out.println(query.toString());
			System.out.println(tuple.deepToString());
			return client.preparedQuery(query.toString()).execute(tuple);
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
					"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveEntityTypes=true")
					.execute();
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForTypesWithDetails(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveEntityTypeDetails=true")
					.execute();
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForType(String tenantId, String type) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveEntityTypeInfo=true AND (C.e_type is NULL OR C.e_type=$1)")
					.execute(Tuple.of(type));
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForAttribs(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveAttrTypes=true")
					.execute();
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForAttribsWithDetails(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveAttrTypeDetails=true")
					.execute();
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForAttrib(String tenantId, String attrib) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveAttrTypeInfo=true AND (C.e_prop is NULL OR C.e_prop=$1) AND (C.e_rel is NULL OR C.e_rel=$1)")
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
				wherePart.append("(C.e_prop is NULL OR C.e_prop IN $");
				wherePart.append(dollarCount);
				wherePart.append(") AND (C.e_rel is NULL OR C.e_rel IN $");
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

	public Uni<Table<String, String, RegistrationEntry>> getAllRegistries() {
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT tenant_id FROM tenant").execute().onItem()
					.transformToUni(tenantRows -> {
						List<Uni<Tuple2<String, RowSet<Row>>>> unis = Lists.newArrayList();
						RowIterator<Row> it = tenantRows.iterator();
						String sql = "SELECT cs_id, c_id, e_id, e_id_p, e_type, e_prop, e_rel, ST_AsGeoJSON(i_location), scopes, EXTRACT(MILLISECONDS FROM expires), endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription FROM csourceinformation WHERE createEntity OR createBatch OR updateEntity OR appendAttrs OR deleteAttrs OR deleteEntity OR upsertBatch OR updateBatch OR deleteBatch";
						unis.add(client.preparedQuery(sql).execute().onItem()
								.transform(rows -> Tuple2.of(AppConstants.INTERNAL_NULL_KEY, rows)));
						while (it.hasNext()) {
							unis.add(clientManager.getClient(it.next().getString(0), false).onItem()
									.transformToUni(tenantClient -> {
										return tenantClient.preparedQuery(sql).execute().onItem().transform(
												tenantReg -> Tuple2.of(AppConstants.INTERNAL_NULL_KEY, tenantReg));
									}));
						}
						return Uni.combine().all().unis(unis).combinedWith(list -> {
							Table<String, String, RegistrationEntry> result = HashBasedTable.create();
							for (Object obj : list) {
								Tuple2<String, RowSet<Row>> tuple = (Tuple2<String, RowSet<Row>>) obj;
								String tenant = tuple.getItem1();
								RowIterator<Row> it2 = tuple.getItem2().iterator();
								while (it2.hasNext()) {
									Row row = it2.next();
									result.put(tenant, row.getString(1), getRegistrationEntry(row, tenant));
								}
							}
							return result;
						});
					});
		});

	}

	private RegistrationEntry getRegistrationEntry(Row row, String tenant) {
//		cs_id bigint,
//		c_id text,  
//		e_id text,
//		e_id_p text,
//		e_type text,
//		e_prop text,
//		e_rel text,
//		i_location GEOMETRY(Geometry, 4326),
//		scopes text[],
//		expires timestamp without time zone,
//		endpoint text,
//		tenant_id text,
//		headers jsonb,
//		reg_mode smallint,
//		createEntity boolean, 13 14 15 16 17 18 19 20
//		updateEntity boolean,
//		appendAttrs boolean,
//		updateAttrs boolean,
//		deleteAttrs boolean,
//		deleteEntity boolean,
//		createBatch boolean,
//		upsertBatch boolean,
//		updateBatch boolean,
//		deleteBatch boolean,
//		upsertTemporal boolean,
//		appendAttrsTemporal boolean,
//		deleteAttrsTemporal boolean,
//		updateAttrsTemporal boolean,
//		deleteAttrInstanceTemporal boolean,
//		deleteTemporal boolean,
//		mergeEntity boolean,
//		replaceEntity boolean,
//		replaceAttrs boolean,
//		mergeBatch boolean,
//		retrieveEntity boolean,
//		queryEntity boolean,
//		queryBatch boolean,
//		retrieveTemporal boolean,
//		queryTemporal boolean,
//		retrieveEntityTypes boolean,
//		retrieveEntityTypeDetails boolean,
//		retrieveEntityTypeInfo boolean,
//		retrieveAttrTypes boolean,
//		retrieveAttrTypeDetails boolean,
//		retrieveAttrTypeInfo boolean,
//		createSubscription boolean,
//		updateSubscription boolean,
//		retrieveSubscription boolean,
//		querySubscription boolean,
//		deleteSubscription boolean,

		// 1 2 3 4 5 6 7geo, 8 scopes 9endpoint 10 tenant 11 headers 12 regMode 13 - ops

		Shape geoJson = null;
		String geoString = row.getString(7);
		if (geoString != null) {
			try {
				geoJson = geoReader.read(geoString);
			} catch (InvalidShapeException | IOException | ParseException e) {
				logger.error("Failed to load registrations for the entity mananger", e);
			}
		}
		return new RegistrationEntry(row.getString(1), row.getString(2), row.getString(3), row.getString(4),
				row.getString(5), row.getString(6), geoJson, row.getArrayOfStrings(8), row.getLong(9),
				row.getInteger(12), row.getBoolean(13), row.getBoolean(14), row.getBoolean(15), row.getBoolean(16),
				row.getBoolean(17), row.getBoolean(18), row.getBoolean(19), row.getBoolean(20), row.getBoolean(21),
				row.getBoolean(22), row.getBoolean(23), row.getBoolean(24), row.getBoolean(25), row.getBoolean(26),
				row.getBoolean(27), row.getBoolean(28), row.getBoolean(29), row.getBoolean(30), row.getBoolean(31),
				row.getBoolean(32), row.getBoolean(33), row.getBoolean(34), row.getBoolean(35), row.getBoolean(36),
				row.getBoolean(37), row.getBoolean(38), row.getBoolean(39), row.getBoolean(40), row.getBoolean(41),
				row.getBoolean(42), row.getBoolean(43), row.getBoolean(44), row.getBoolean(45), row.getBoolean(46),
				row.getBoolean(47), row.getBoolean(48),
				new RemoteHost(row.getString(9), row.getString(10),
						MultiMap.newInstance(
								HttpUtils.getHeadersForRemoteCall(row.getJsonArray(11), row.getString(10))),
						row.getString(1), false, false));

	}

}
