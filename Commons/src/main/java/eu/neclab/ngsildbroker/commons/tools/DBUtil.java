package eu.neclab.ngsildbroker.commons.tools;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.List;

import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.io.GeoJSONReader;
import org.locationtech.spatial4j.shape.Shape;
import org.slf4j.Logger;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.RowSet;

public class DBUtil {

	public static GeoJSONReader geoReader = new GeoJSONReader(JtsSpatialContext.GEO, new SpatialContextFactory());

	public static String databaseURLFromPostgresJdbcUrl(String url, String newDbName) {
		try {
			String cleanURI = url.substring(5);

			URI uri = URI.create(cleanURI);
			return "jdbc:" + uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort() + "/" + newDbName;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static Uni<RegistrationEntry> getRegistrationEntry(Row row, String tenant, JsonLDService ldService,
			Logger logger) {
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
//		queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap
		// 1 2 3 4 5 6 7geo, 8 scopes 9endpoint 10 tenant 11 headers 12 regMode 13 - ops
		MultiMap headers = MultiMap
				.newInstance(HttpUtils.getHeadersForRemoteCall(row.getJsonArray(12), row.getString(11)));
		String atContextLink = headers.get(NGSIConstants.JSONLD_CONTEXT);
		Uni<Context> ctxUni;
		if (atContextLink == null) {
			ctxUni = Uni.createFrom().nullItem();
		} else {
			headers.remove(NGSIConstants.JSONLD_CONTEXT);
			ctxUni = ldService.parse(atContextLink);
		}
		return ctxUni.onItem().transform(ctx -> {
			Shape geoJson = null;
			String geoString = row.getString(7);
			if (geoString != null) {
				try {
					geoJson = geoReader.read(geoString);

				} catch (InvalidShapeException | IOException | ParseException e) {
					logger.error("Failed to load registrations for the entity mananger", e);
				}
			}
			Long expires = row.getLong(9);
			if (expires == null) {
				expires = -1l;
			}

			return new RegistrationEntry(row.getString(1), row.getString(2), row.getString(3), row.getString(4),
					row.getString(5), row.getString(6), geoJson, row.getArrayOfStrings(8), expires, row.getInteger(13),
					row.getBoolean(14), row.getBoolean(15), row.getBoolean(16), row.getBoolean(17), row.getBoolean(18),
					row.getBoolean(19), row.getBoolean(20), row.getBoolean(21), row.getBoolean(22), row.getBoolean(23),
					row.getBoolean(24), row.getBoolean(25), row.getBoolean(26), row.getBoolean(27), row.getBoolean(28),
					row.getBoolean(29), row.getBoolean(30), row.getBoolean(31), row.getBoolean(32), row.getBoolean(33),
					row.getBoolean(34), row.getBoolean(35), row.getBoolean(36), row.getBoolean(37), row.getBoolean(38),
					row.getBoolean(39), row.getBoolean(40), row.getBoolean(41), row.getBoolean(42), row.getBoolean(43),
					row.getBoolean(44), row.getBoolean(45), row.getBoolean(46), row.getBoolean(47), row.getBoolean(48),
					row.getBoolean(49), row.getBoolean(50), row.getBoolean(51), row.getBoolean(52), row.getBoolean(53),
					row.getBoolean(54), new RemoteHost(row.getString(10), row.getString(11), headers, row.getString(1),
							false, false, row.getInteger(13), false, false),
					ctx);
		});

	}

	public static Uni<Table<String, String, List<RegistrationEntry>>> getAllRegistries(ClientManager clientManager,
			JsonLDService ldService, String sql, Logger logger) {
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT tenant_id FROM tenant").execute().onItem()
					.transformToUni(tenantRows -> {
						List<Uni<Tuple2<String, RowSet<Row>>>> unis = Lists.newArrayList();
						RowIterator<Row> it = tenantRows.iterator();
						// String sql = "SELECT cs_id, c_id, e_id, e_id_p, e_type, e_prop, e_rel,
						// ST_AsGeoJSON(i_location), scopes, EXTRACT(MILLISECONDS FROM expires),
						// endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity,
						// appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch,
						// upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal,
						// deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal,
						// deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch,
						// retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal,
						// retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo,
						// retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo,
						// createSubscription, updateSubscription, retrieveSubscription,
						// querySubscription, deleteSubscription, queryEntityMap, createEntityMap,
						// updateEntityMap, deleteEntityMap, retrieveEntityMap FROM csourceinformation
						// WHERE queryentity OR querybatch OR retrieveentity OR retrieveentitytypes OR
						// retrieveentitytypedetails OR retrieveentitytypeinfo OR retrieveattrtypes OR
						// retrieveattrtypedetails OR retrieveattrtypeinfo";
						unis.add(client.preparedQuery(sql).execute().onItem()
								.transform(rows -> Tuple2.of(AppConstants.INTERNAL_NULL_KEY, rows)));
						while (it.hasNext()) {
							unis.add(clientManager.getClient(it.next().getString(0), false).onItem()
									.transformToUni(tenantClient -> {
										return tenantClient.preparedQuery(sql).execute().onItem().transform(
												tenantReg -> Tuple2.of(AppConstants.INTERNAL_NULL_KEY, tenantReg));
									}));
						}
						return Uni.combine().all().unis(unis).with(list -> {
							Table<String, String, List<RegistrationEntry>> result = HashBasedTable.create();
							List<Uni<Void>> regEntries = Lists.newArrayList();
							for (Object obj : list) {
								@SuppressWarnings("unchecked")
								Tuple2<String, RowSet<Row>> tuple = (Tuple2<String, RowSet<Row>>) obj;
								String tenant = tuple.getItem1();
								RowIterator<Row> it2 = tuple.getItem2().iterator();
								while (it2.hasNext()) {
									Row row = it2.next();
									String cId = row.getString(1);
									List<RegistrationEntry> entries;
									if (result.contains(tenant, cId)) {
										entries = result.get(tenant, cId);
									} else {
										entries = Lists.newArrayList();
										result.put(tenant, cId, entries);
									}

									regEntries.add(DBUtil.getRegistrationEntry(row, tenant, ldService, logger).onItem()
											.transformToUni(regEntry -> {
												entries.add(regEntry);
												return Uni.createFrom().voidItem();
											}));

								}
							}

							return Tuple2.of(result, regEntries);
						}).onItem().transformToUni(tpl -> {
							if (tpl.getItem2().isEmpty()) {
								return Uni.createFrom().item(tpl.getItem1());
							}
							return Uni.combine().all().unis(tpl.getItem2()).with(v -> tpl.getItem1());
						});
					});
		});

	}

}