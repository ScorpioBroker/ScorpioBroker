package eu.neclab.ngsildbroker.commons.serialization;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import com.github.filosganga.geogson.gson.GeometryAdapterFactory;
import com.github.filosganga.geogson.jts.JtsAdapterFactory;
import com.github.filosganga.geogson.model.Geometry;
import com.github.filosganga.geogson.model.MultiPolygon;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import eu.neclab.ngsildbroker.commons.datatypes.BatchResult;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.datatypes.GeoValue;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.TemporalEntityStorageKey;
import eu.neclab.ngsildbroker.commons.datatypes.TypedValue;

public class DataSerializer {

	// private static final Type INDEX_SET_TYPE = new TypeToken<Set<Index>>() {
	// }.getType();
	private static final Gson GSON = createGsonObject();
	private static final Gson SPECIAL_GSON = createSpecialGsonObject();

	// private static final Type propertiesType = new TypeToken<List<Property>>() {
	//
	// }.getType();

	public static void main(String[] args) {
		Entity entity = getEntity("{\r\n" + "    \"http://schema.org/vehicle/brandName\": [\r\n" + "      {\r\n"
				+ "        \"@type\": [\r\n" + "          \"http://schema.org/ngsi-ld/Property\"\r\n" + "        ],\r\n"
				+ "        \"http://schema.org/ngsi-ld/hasValue\": [\r\n" + "          {\r\n"
				+ "            \"@value\": \"Mercedes\"\r\n" + "          }\r\n" + "        ]\r\n" + "      }\r\n"
				+ "    ],\r\n" + "    \"@id\": \"urn:ngsi-ld:Vehicle:A4010\",\r\n"
				+ "    \"http://schema.org/common/isParked\": [\r\n" + "      {\r\n"
				+ "        \"http://schema.org/ngsi-ld/observedAt\": [\r\n" + "          {\r\n"
				+ "            \"@type\": \"http://schema.org/ngsi-ld/DateTime\",\r\n"
				+ "            \"@value\": \"2017-07-29T12:00:04\"\r\n" + "          }\r\n" + "        ],\r\n"
				+ "        \"http://schema.org/common/providedBy\": [\r\n" + "          {\r\n"
				+ "            \"http://schema.org/ngsi-ld/hasObject\": [\r\n" + "              {\r\n"
				+ "                \"@id\": \"urn:ngsi-ld:Person:Bob\"\r\n" + "              }\r\n"
				+ "            ],\r\n" + "            \"@type\": [\r\n"
				+ "              \"http://schema.org/ngsi-ld/Relationship\"\r\n" + "            ]\r\n"
				+ "          }\r\n" + "        ],\r\n" + "        \"@type\": [\r\n"
				+ "          \"http://schema.org/ngsi-ld/Relationship\"\r\n" + "        ],\r\n"
				+ "        \"http://schema.org/ngsi-ld/hasValue\": [\r\n" + "          {\r\n"
				+ "            \"@value\": \"urn:ngsi-ld:OffStreetParking:Downtown1\"\r\n" + "          }\r\n"
				+ "        ]\r\n" + "      }\r\n" + "    ],\r\n" + "    \"@type\": [\r\n"
				+ "      \"http://schema.org/vehicle/Vehicle\"\r\n" + "    ]\r\n" + "}");
		System.out.println("Entity :: " + entity);
		/*
		 * for(Property p:entity.getProperties()) {
		 * System.out.println("P : "+p.getValue()); }
		 */
		System.out.println("Json simplified ::" + toJson(entity));
	}

	private DataSerializer() {
		// Do nothing. (prevent instantiation)
	}

	private static Gson createGsonObject() {
		GsonBuilder builder = new GsonBuilder();
		registerTypes(builder);
		return builder.setPrettyPrinting().create();
	}

	private static Gson createSpecialGsonObject() {
		GsonBuilder builder = new GsonBuilder();
		builder.registerTypeAdapter(Entity.class, new EntityGsonAdapter(true));
		return builder.setPrettyPrinting().create();
	}

	private static void registerTypes(GsonBuilder builder) {
		// Index metadata

		builder.registerTypeAdapter(Entity.class, new EntityGsonAdapter(false));
		builder.registerTypeAdapter(Subscription.class, new SubscriptionGsonAdapter());
		builder.registerTypeAdapter(CSourceRegistration.class, new CSourceRegistrationGsonAdapter());
		builder.registerTypeAdapter(GeoValue.class, new GeoValueGsonAdapter());
		builder.registerTypeAdapter(BatchResult.class, new BatchResultGsonAdapter());
		builder.registerTypeAdapterFactory(new GeometryAdapterFactory());
		builder.registerTypeAdapterFactory(new JtsAdapterFactory());
		builder.registerTypeAdapter(Notification.class, new NotificationGsonAdapter());
		builder.registerTypeAdapter(TypedValue.class, new TypedValueGsonAdapter());
		builder.registerTypeAdapter(SerializationTypes.entitiesType, new EntitiesGsonAdapter());
		// builder.registerTypeAdapter(propertiesType, new PropertiesGsonAdapter());
	}

	public static List<Entity> getEntities(String json) {
		return GSON.fromJson(json, SerializationTypes.entitiesType);
	}

	public static List<Entity> getEntities(InputStreamReader in) {
		return GSON.fromJson(in, SerializationTypes.entitiesType);
	}

	public static Entity getEntity(String json) {
		return GSON.fromJson(json, Entity.class);
	}

	public static Subscription getSubscription(String json) {
		return GSON.fromJson(json, Subscription.class);
	}

	public static SubscriptionRequest getSubscriptionRequest(String json) {
		return GSON.fromJson(json, SubscriptionRequest.class);
	}

	public static Notification getNotification(String json) {
		return GSON.fromJson(json, Notification.class);
	}

	public static CSourceRegistration getCSourceRegistration(String json) {
		return GSON.fromJson(json, CSourceRegistration.class);
	}

	public static GeoValue getGeoValue(String json) {
		return GSON.fromJson(json, GeoValue.class);
	}

	public static Geometry<?> getGeojsonGeometry(String json) {
		return GSON.fromJson(json, Geometry.class);
	}

	// public static List<Property> getProperties(String json){
	// return GSON.fromJson(json, propertiesType);
	// }

	// get collection of entities from json
	public static List<CSourceRegistration> getCSourceRegistrations(String json, Type type) {
		return GSON.fromJson(json, type);
	}

	public static String toJson(Object obj) {
		return GSON.toJson(obj);
	}

	public static Entity getPartialEntity(String json) {
		return SPECIAL_GSON.fromJson(json, Entity.class);
	}

	public static QueryParams getQueryParams(String json) {
		return GSON.fromJson(json, QueryParams.class);
	}

	public static ArrayList<String> getStringList(String json) {
		return GSON.fromJson(json, ArrayList.class);
	}

	public static TemporalEntityStorageKey getTemporalEntityStorageKey(String json) {
		return GSON.fromJson(json, TemporalEntityStorageKey.class);
	}

}
