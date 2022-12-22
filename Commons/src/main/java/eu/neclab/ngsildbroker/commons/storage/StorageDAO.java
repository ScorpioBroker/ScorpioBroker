package eu.neclab.ngsildbroker.commons.storage;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.datatypes.HistoryAttribInstance;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.pgclient.PgException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public abstract class StorageDAO {
	private final static Logger logger = LoggerFactory.getLogger(StorageDAO.class);

	@Inject
	protected ClientManager clientManager;
	StorageFunctionsInterface storageFunctions;

	protected abstract StorageFunctionsInterface getStorageFunctions();

	@PostConstruct
	public void init() {
		storageFunctions = getStorageFunctions();
	}

	public Uni<ArrayListMultimap<String, String>> getAllIds() {
		List<Uni<Object>> unis = Lists.newArrayList();
		for (Entry<String, Uni<PgPool>> entry : clientManager.getAllClients().entrySet()) {
			Uni<PgPool> clientUni = entry.getValue();
			String tenant = entry.getKey();
			unis.add(clientUni.onItem().transformToUni(client -> client.query(storageFunctions.getAllIdsQuery())
					.execute().onItem().transform(t -> Tuple2.of(t, tenant))));
		}
		return Uni.combine().all().unis(unis).combinedWith(t -> {
			ArrayListMultimap<String, String> result = ArrayListMultimap.create();
			for (Object item : t) {
				Tuple2<RowSet<Row>, String> tuple = (Tuple2<RowSet<Row>, String>) item;
				tuple.getItem1().forEach(t2 -> {
					result.put(tuple.getItem2(), t2.getString(0));
				});
			}
			return result;
		});

	}

	public Uni<Map<String, Object>> getEntity(String entryId, String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(storageFunctions.getEntryQuery()).execute(Tuple.of(entryId)).onItem()
					.transformToUni(t -> {
						if (t.rowCount() == 0) {
							return Uni.createFrom()
									.failure(new ResponseException(ErrorType.NotFound, entryId + " was not found"));
						}
						return Uni.createFrom().item(t.iterator().next().getJsonObject(0).getMap());
					});
		});

	}

	public Uni<String> getEndpoint(String entityId, String tenantId) {
		String query = "SELECT endpoint FROM csource cs, csourceinformation csi WHERE cs.id=csi.csource_id AND csi.entity_id='"
				+ entityId + "'";
		return clientManager.getClient(tenantId, false).onItem()
				.transformToUni(client -> client.preparedQuery(query).execute().onItem().transform((rowSet) -> {
					if (rowSet.rowCount() == 0) {
						return null;
					}
					return rowSet.iterator().next().getString("endpoint");
				}).onFailure().recoverWithUni(Uni.createFrom().item("")));
	}

	public Uni<Map<String, List<Map<String, Object>>>> getAllEntities() {
		List<Uni<Object>> unis = Lists.newArrayList();
		for (Entry<String, Uni<PgPool>> entry : clientManager.getAllClients().entrySet()) {
			Uni<PgPool> clientUni = entry.getValue();
			String tenant = entry.getKey();
			unis.add(clientUni.onItem().transformToUni(client -> client.query("SELECT data FROM entity").execute()
					.onItem().transform(t -> Tuple2.of(t, tenant))));
		}
		return Uni.combine().all().unis(unis).combinedWith(t -> {
			Map<String, List<Map<String, Object>>> result = Maps.newHashMap();

			for (Object item : t) {
				Tuple2<RowSet<Row>, String> tuple = (Tuple2<RowSet<Row>, String>) item;
				List<Map<String, Object>> tmp = Lists.newArrayList();
				tuple.getItem1().forEach(t2 -> {
					tmp.add(t2.getJsonObject(0).getMap());
				});
				result.put(tuple.getItem2(), tmp);
			}
			return result;
		});
	}

	public Uni<QueryResult> query(QueryParams qp) {
		return clientManager.getClient(qp.getTenant(), false).onItem().transformToUni(client -> {

			if (qp.getCheck() != null) {
				logger.debug("Query for types or attributes got called");
				return client.withTransaction(conn -> {
					return storageFunctions.typesAndAttributeQuery(qp, conn).onItem().transform(rows -> {
						QueryResult queryResult = new QueryResult(null, null, ErrorType.None, -1, true);
						List<Map<String, Object>> list = Lists.newArrayList();
						if (rows != null) {
							rows.forEach(t -> {
								list.add(t.getJsonObject(0).getMap());
							});
						}
						queryResult.setData(list);
						return queryResult;
					});
				});
			}
			return client.withTransaction(conn -> {
				if (qp.getCountResult()) {
					if (qp.getLimit() == 0) {
						logger.debug("Query for count got called");
						return storageFunctions.translateNgsildQueryToCountResult(qp, conn).onItem().transform(t -> {
							QueryResult queryResult = new QueryResult(null, null, ErrorType.None, -1, true);
							queryResult.setCount(t.iterator().next().getLong(0));
							return queryResult;
						});
					}
					return storageFunctions.translateNgsildQueryToSql(qp, conn).onItem().transform(t -> {
						QueryResult queryResult = new QueryResult(null, null, ErrorType.None, -1, true);
						List<Map<String, Object>> list = Lists.newArrayList();
						Long count = 0l;
						for (Row e : t) {
							list.add(e.getJsonObject("data").getMap());
							count = e.getLong("count");
						}
						queryResult.setCount(count);
						queryResult.setOffset(qp.getOffSet());
						queryResult.setLimit(qp.getLimit());
						long after = count - qp.getOffSet() - qp.getLimit();
						if (after < 0) {
							after = 0;
						}
						queryResult.setResultsLeftAfter(after);
						long before = count - qp.getOffSet();
						if (before < 0) {
							before = 0;
						}
						queryResult.setResultsLeftBefore(before);
						queryResult.setData(list);
						return queryResult;
					});
				} else {
					return storageFunctions.translateNgsildQueryToSql(qp, conn).onFailure().recoverWithUni(t -> {
						String sqlCode = ((PgException) t).getCode();
						if (sqlCode.equals("2201B")) {
							return Uni.createFrom().failure(
									new ResponseException(ErrorType.BadRequestData, "Invalid regular expression"));
						} else {
							return Uni.createFrom().failure(t);
						}
					}).onItem().transform(t -> {
						QueryResult queryResult = new QueryResult(null, null, ErrorType.None, -1, true);
						List<Map<String, Object>> list = Lists.newArrayList();
						t.forEach(e -> {
							list.add(e.getJsonObject(0).getMap());
						});
						queryResult.setData(list);
						queryResult.setOffset(qp.getOffSet());
						queryResult.setLimit(qp.getLimit());
						long after;
						if (list.size() < qp.getLimit()) {
							after = 0;
						} else {
							after = qp.getLimit() + 1;
						}
						long before = qp.getOffSet();

						queryResult.setResultsLeftAfter(after);
						queryResult.setResultsLeftBefore(before);
						return queryResult;
					});
				}

			});
		});
	}

	public Uni<CreateResult> storeTemporalEntity(HistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {

			if (request instanceof DeleteHistoryEntityRequest) {
				return doTemporalSqlAttrInsert(client, JsonObject.mapFrom(null), request.getId(), request.getType(),
						((DeleteHistoryEntityRequest) request).getResolvedAttrId(), request.getCreatedAt(),
						request.getModifiedAt(), ((DeleteHistoryEntityRequest) request).getInstanceId(), null);
			} else {
				List<Uni<CreateResult>> unis = Lists.newArrayList();
				for (HistoryAttribInstance entry : request.getAttribs()) {
					unis.add(doTemporalSqlAttrInsert(client, entry.getElementValue(), entry.getEntityId(),
							entry.getEntityType(), entry.getAttributeId(), entry.getEntityCreatedAt(),
							entry.getEntityModifiedAt(), entry.getInstanceId(), entry.getOverwriteOp()));
				}
				if (unis.isEmpty()) {
					return Uni.createFrom().item(new CreateResult(request.getId(), false));
				}
				return Uni.combine().all().unis(unis).combinedWith(t -> {
					CreateResult result = new CreateResult("", false);
					for (Object entry : t) {
						CreateResult tmp = (CreateResult) entry;
						result.setEntityId(tmp.getEntityId());
						result.setCreatedOrUpdated(result.isCreatedOrUpdated() && tmp.isCreatedOrUpdated());
					}
					return result;
				});
			}
		});

	}

	public Uni<Void> storeRegistryEntry(CSourceRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			JsonObject value = request.getResultCSourceRegistrationString();
			String sql;

			if (value != null) {
				if (request.getRequestType() == AppConstants.CREATE_REQUEST) {

					sql = "INSERT INTO " + DBConstants.DBTABLE_CSOURCE + " (id, " + DBConstants.DBCOLUMN_DATA
							+ ") VALUES ($1, $2::jsonb)";

					return client.preparedQuery(sql).execute(Tuple.of(request.getId(), value)).onFailure()
							.recoverWithUni(e -> {
								if (e instanceof PgException) {
									PgException pgE = (PgException) e;
									if (pgE.getCode().equals("23505")) { // code for unique constraint
										return Uni.createFrom().failure(new ResponseException(ErrorType.AlreadyExists,
												request.getId() + " already exists"));
									}
								}
								return Uni.createFrom().failure(e);
							}).onItem().transformToUni(t -> Uni.createFrom().voidItem());

				} else if (request.getRequestType() == AppConstants.APPEND_REQUEST) {
					sql = "INSERT INTO " + DBConstants.DBTABLE_CSOURCE + " (id, " + DBConstants.DBCOLUMN_DATA
							+ ") VALUES ($1, $2::jsonb) ON CONFLICT(id) DO UPDATE SET " + DBConstants.DBCOLUMN_DATA
							+ " = EXCLUDED." + DBConstants.DBCOLUMN_DATA;
					return client.preparedQuery(sql).execute(Tuple.of(request.getId(), value)).onFailure()
							.recoverWithUni(e -> {
								return Uni.createFrom()
										.failure(new ResponseException(ErrorType.InternalError, e.getMessage()));
							}).onItem().ignore().andContinueWithNull();
				} else {
					return Uni.createFrom()
							.failure(new ResponseException(ErrorType.InternalError, "was not executed unknown call"));
				}
			} else {
				sql = "DELETE FROM " + DBConstants.DBTABLE_CSOURCE + " WHERE id = $1";
				return client.preparedQuery(sql).execute(Tuple.of(request.getId())).onFailure().recoverWithUni(e -> {
					return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError, e.getMessage()));
				}).onItem().ignore().andContinueWithNull();
			}
		});

	}

	private Uni<CreateResult> doTemporalSqlAttrInsert(PgPool client, JsonObject value, String entityId,
			String entityType, String attributeId, String entityCreatedAt, String entityModifiedAt, String instanceId,
			Boolean overwriteOp) {
		if (value != null) {
			return client.withTransaction(conn -> {
				String sql;
				List<Uni<Boolean>> unis = Lists.newArrayList();
				if (entityId != null && entityType != null && entityCreatedAt != null && entityModifiedAt != null) {
					sql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY
							+ " (id, type, createdat, modifiedat) VALUES($1, $2, $3::timestamp, $4::timestamp) "
							+ "ON CONFLICT(id) DO UPDATE SET type = EXCLUDED.type, createdat = EXCLUDED.createdat, modifiedat = EXCLUDED.modifiedat RETURNING id";
					Tuple tValue = Tuple.of(entityId, entityType,
							SerializationTools.localDateTimeFormatter(entityCreatedAt),
							SerializationTools.localDateTimeFormatter(entityModifiedAt));
					unis.add(conn.preparedQuery(sql).execute(tValue).onItem().transform(tmp -> {
						if (tmp.size() == 0) {
							return false;
						}
						return true;
					}));
				}
				if (entityId != null && attributeId != null) {
					if (overwriteOp != null && overwriteOp) {
						sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
								+ " WHERE temporalentity_id = $1 AND attributeid = $2";
						unis.add(conn.preparedQuery(sql).execute(Tuple.of(entityId, attributeId)).onItem()
								.transform(t -> false));
					}
					sql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
							+ " (temporalentity_id, attributeid, data) VALUES ($1, $2, $3::jsonb) ON CONFLICT(temporalentity_id, attributeid, instanceid) DO UPDATE SET data = EXCLUDED.data";
					unis.add(conn.preparedQuery(sql).execute(Tuple.of(entityId, attributeId, value)).onItem()
							.transform(t -> false));
					// update modifiedat field in temporalentity
					sql = "UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY
							+ " SET modifiedat = $1::timestamp WHERE id = $2";
					unis.add(conn.preparedQuery(sql)
							.execute(Tuple.of(SerializationTools.localDateTimeFormatter(entityModifiedAt), entityId))
							.onItem().transform(t -> false));
				}

				return Uni.combine().all().unis(unis).combinedWith(list -> {
					CreateResult result = new CreateResult(entityId, false);
					for (Object entry : list) {
						result.setCreatedOrUpdated(result.isCreatedOrUpdated() && (Boolean) entry);
					}
					return result;
				}).onFailure().recoverWithUni(t -> {
					if (t instanceof SQLIntegrityConstraintViolationException) {
						List<Uni<Boolean>> recoverUnis = Lists.newArrayList();
						logger.info("Failed to create attribute instance because of data inconsistency");
						logger.info("Attempting recovery");
						String selectSql = "SELECT type, createdat, modifiedat FROM " + DBConstants.DBTABLE_ENTITY
								+ " WHERE id = $1";
						return conn.preparedQuery(selectSql).execute(Tuple.of(entityId)).onItem()
								.transformToUni(rows -> {
									if (rows.size() == 0) {
										logger.error("Recovery failed");
										return Uni.createFrom().failure(t);
									}
									Row row = rows.iterator().next();
									String recoverSql;
									recoverSql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY
											+ " (id, type, createdat, modifiedat) VALUES ($1, $2, $3::timestamp, $4::timestamp) ON CONFLICT(id) DO UPDATE SET type = EXCLUDED.type, createdat = EXCLUDED.createdat, modifiedat = EXCLUDED.modifiedat RETURNING id";
									recoverUnis.add(conn.preparedQuery(recoverSql)
											.execute(Tuple.of(entityId, row.getString("type"),
													row.getString("createdat"), row.getString("modifiedat")))
											.onItem().transform(tmp -> {
												if (tmp.size() == 0) {
													return false;
												}
												return true;
											}));
									recoverSql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
											+ " (temporalentity_id, attributeid, data) VALUES ($1, $2, '" + value
											+ "'::jsonb) ON CONFLICT(temporalentity_id, attributeid, instanceid) DO UPDATE SET data = EXCLUDED.data";
									recoverUnis.add(conn.preparedQuery(recoverSql)
											.execute(Tuple.of(entityId, attributeId)).onItem().transform(t2 -> false));
									// update modifiedat field in temporalentity
									recoverSql = "UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY + " SET modifiedat = '"
											+ entityModifiedAt + "'::timestamp WHERE id = $1";
									recoverUnis.add(conn.preparedQuery(recoverSql).execute(Tuple.of(entityId)).onItem()
											.transform(t2 -> false));
									logger.info("Recovery successful");
									return Uni.combine().all().unis(recoverUnis).combinedWith(list -> {
										CreateResult result = new CreateResult(entityId, false);
										for (Object entry : list) {
											result.setCreatedOrUpdated(result.isCreatedOrUpdated() && (Boolean) entry);
										}
										return result;
									});
								});
					} else {
						logger.error("Recovery failed", t);
						return Uni.createFrom().failure(t);
					}

				});
			});
		} else {
			String sql;
			if (entityId != null && attributeId != null && instanceId != null) {
				sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
						+ " WHERE temporalentity_id = $1 AND attributeid = $2 AND instanceid = $3";
				return client.preparedQuery(sql).execute(Tuple.of(entityId, attributeId, instanceId)).onItem()
						.transformToUni(t -> {
							if (t.rowCount() == 0) {
								return Uni.createFrom().failure(
										new ResponseException(ErrorType.NotFound, instanceId + " was not found"));
							}
							return Uni.createFrom().nullItem();
						});

			} else if (entityId != null && attributeId != null) {
				sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
						+ " WHERE temporalentity_id = $1 AND attributeid = $2";
				return client.preparedQuery(sql).execute(Tuple.of(entityId, attributeId)).onItem().transformToUni(t -> {
					if (t.rowCount() == 0) {
						return Uni.createFrom()
								.failure(new ResponseException(ErrorType.NotFound, attributeId + " was not found"));
					}
					return Uni.createFrom().nullItem();
				});
			} else if (entityId != null) {
				sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY + " WHERE id = $1";
				return client.preparedQuery(sql).execute(Tuple.of(entityId)).onItem().transformToUni(t -> {
					if (t.rowCount() == 0) {
						return Uni.createFrom()
								.failure(new ResponseException(ErrorType.NotFound, entityId + " was not found"));
					}
					return Uni.createFrom().nullItem();
				});
			}
			return Uni.createFrom().nullItem();
		}

	}

	public Uni<Void> storeEntity(EntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			String sql;
			String key = request.getId();
			JsonObject value = request.getWithSysAttrs();
			JsonObject valueWithoutSysAttrs = request.getEntityWithoutSysAttrs();
			JsonObject kvValue = request.getKeyValue();
			if (value != null) {
				if (request.getRequestType() == AppConstants.OPERATION_CREATE_ENTITY) {
					sql = "INSERT INTO " + DBConstants.DBTABLE_ENTITY + " (id, " + DBConstants.DBCOLUMN_DATA + ", "
							+ DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS + ",  " + DBConstants.DBCOLUMN_KVDATA
							+ ") VALUES ($1, $2::jsonb, $3::jsonb, $4::jsonb)";
					return client.preparedQuery(sql).execute(Tuple.of(key, value, valueWithoutSysAttrs, kvValue))
							.onFailure().retry().atMost(3).onFailure().recoverWithUni(e -> {
								if (e instanceof PgException) {
									PgException pgE = (PgException) e;
									if (pgE.getCode().equals("23505")) { // code for unique constraint
										return Uni.createFrom().failure(new ResponseException(ErrorType.AlreadyExists,
												request.getId() + " already exists"));
									}
								}
								return Uni.createFrom().failure(e);

							}).onItem().ignore().andContinueWithNull();
				} else {
					sql = "UPDATE " + DBConstants.DBTABLE_ENTITY + " SET " + DBConstants.DBCOLUMN_DATA
							+ " = $1::jsonb , " + DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS + " = $2::jsonb , "
							+ DBConstants.DBCOLUMN_KVDATA + " = $3::jsonb WHERE " + DBConstants.DBCOLUMN_ID + " = $4";
					return client.preparedQuery(sql).execute(Tuple.of(value, valueWithoutSysAttrs, kvValue, key))
							.onFailure().retry().atMost(3).onItem().ignore().andContinueWithNull();
				}
			} else {

				sql = "DELETE FROM " + DBConstants.DBTABLE_ENTITY + " WHERE id = $1";
				return client.preparedQuery(sql).execute(Tuple.of(key)).onFailure().retry().atMost(3).onItem()
						.transformToUni(t -> {
							// if(t.)
							return Uni.createFrom().nullItem();
						});
			}
		});
	}

}
