package eu.neclab.ngsildbroker.commons.tools;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.io.GeoJSONReader;
import org.locationtech.spatial4j.shape.Shape;
import org.slf4j.Logger;

import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.sqlclient.Row;

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

	public static RegistrationEntry getRegistrationEntry(Row row, String tenant, Logger logger) {
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
				row.getBoolean(54),
				new RemoteHost(row.getString(10), row.getString(11),
						MultiMap.newInstance(
								HttpUtils.getHeadersForRemoteCall(row.getJsonArray(12), row.getString(11))),
						row.getString(1), false, false, row.getInteger(13), false, false));

	}

}