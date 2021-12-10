package eu.neclab.ngsildbroker.commons.serialization;

import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import com.github.filosganga.geogson.gson.GeometryAdapterFactory;
import com.github.filosganga.geogson.jts.JtsAdapterFactory;
import com.github.filosganga.geogson.model.Geometry;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import eu.neclab.ngsildbroker.commons.datatypes.AppendCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.AppendEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.BatchResult;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.CreateCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.DeleteCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.DeleteEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.datatypes.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.GeoValue;
import eu.neclab.ngsildbroker.commons.datatypes.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.TemporalEntityStorageKey;
import eu.neclab.ngsildbroker.commons.datatypes.TypedValue;
import eu.neclab.ngsildbroker.commons.datatypes.UpdateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.UpdateHistoryEntityRequest;

public class DataSerializer {

	// private static final Type INDEX_SET_TYPE = new TypeToken<Set<Index>>() {
	// }.getType();
	private static final Gson GSON = createGsonObject();
	private static final Gson SPECIAL_GSON = createSpecialGsonObject();

	// private static final Type propertiesType = new TypeToken<List<Property>>() {
	//
	// }.getType();



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
		builder.registerTypeAdapter(EntityRequest.class, new EntityRequestGsonAdapter());
		builder.registerTypeAdapter(CreateEntityRequest.class, new EntityRequestGsonAdapter());
		builder.registerTypeAdapter(UpdateEntityRequest.class, new EntityRequestGsonAdapter());
		builder.registerTypeAdapter(AppendEntityRequest.class, new EntityRequestGsonAdapter());
		builder.registerTypeAdapter(DeleteEntityRequest.class, new EntityRequestGsonAdapter());
		builder.registerTypeAdapter(CreateHistoryEntityRequest.class, new HistoryEntityRequestGsonAdapter());
		builder.registerTypeAdapter(UpdateHistoryEntityRequest.class, new HistoryEntityRequestGsonAdapter());
		builder.registerTypeAdapter(AppendHistoryEntityRequest.class, new HistoryEntityRequestGsonAdapter());
		builder.registerTypeAdapter(DeleteHistoryEntityRequest.class, new HistoryEntityRequestGsonAdapter());
		builder.registerTypeAdapter(SubscriptionRequest.class, new SubscriptionRequestGsonAdapter());
		builder.registerTypeAdapter(SerializationTypes.entitiesType, new EntitiesGsonAdapter());
		// builder.registerTypeAdapter(propertiesType, new PropertiesGsonAdapter());
		builder.registerTypeAdapter(CreateCSourceRequest.class, new CSourceRequestGsonAdapter());
		builder.registerTypeAdapter(CSourceRequest.class, new CSourceRequestGsonAdapter());
		builder.registerTypeAdapter(AppendCSourceRequest.class, new CSourceRequestGsonAdapter());
		builder.registerTypeAdapter(DeleteCSourceRequest.class, new CSourceRequestGsonAdapter());

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

	public static EntityRequest getEntityRequest(String json) {
		return GSON.fromJson(json, EntityRequest.class);
	}

	public static CreateEntityRequest getCreateEntityRequest(String json) {
		return GSON.fromJson(json, CreateEntityRequest.class);
	}

	public static UpdateEntityRequest getUpdateEntityRequest(String json) {
		return GSON.fromJson(json, UpdateEntityRequest.class);
	}

	public static AppendEntityRequest getAppendEntityRequest(String json) {
		return GSON.fromJson(json, AppendEntityRequest.class);
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

	@SuppressWarnings("unchecked")//Always a String list. No class object from generics
	public static ArrayList<String> getStringList(String json) {
		return GSON.fromJson(json, ArrayList.class);
	}

	public static TemporalEntityStorageKey getTemporalEntityStorageKey(String json) {
		return GSON.fromJson(json, TemporalEntityStorageKey.class);
	}
	// ---------------------------------------------------------------------------------------

	public static CSourceRequest getCSourceRequest(String json) {
		return GSON.fromJson(json, CSourceRequest.class);
	}

	public static CreateCSourceRequest getCreateCSourceRequest(String json) {
		return GSON.fromJson(json, CreateCSourceRequest.class);
	}

	public static AppendCSourceRequest getAppendCSourceRequest(String json) {
		return GSON.fromJson(json, AppendCSourceRequest.class);
	}

	public static DeleteCSourceRequest getDeleteCSourceRequest(String json) {
		return GSON.fromJson(json, DeleteCSourceRequest.class);
	}

	public static HistoryEntityRequest getHistoryEntityRequest(String json) {
		return GSON.fromJson(json, HistoryEntityRequest.class);
	}
	// ------------------------------------------------------------------------------------------

}
