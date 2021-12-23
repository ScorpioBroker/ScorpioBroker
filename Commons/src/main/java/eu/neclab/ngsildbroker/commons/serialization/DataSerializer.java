package eu.neclab.ngsildbroker.commons.serialization;

import com.github.filosganga.geogson.gson.GeometryAdapterFactory;
import com.github.filosganga.geogson.jts.JtsAdapterFactory;
import com.github.filosganga.geogson.model.Geometry;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import eu.neclab.ngsildbroker.commons.datatypes.BatchResult;
import eu.neclab.ngsildbroker.commons.datatypes.GeoValue;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRequest;

public class DataSerializer {

	private static final Gson GSON = createGsonObject();

	private DataSerializer() {
		// Do nothing. (prevent instantiation)
	}

	private static Gson createGsonObject() {
		GsonBuilder builder = new GsonBuilder();
		registerTypes(builder);
		return builder.setPrettyPrinting().create();
	}

	private static void registerTypes(GsonBuilder builder) {
		// Index metadata

		builder.registerTypeAdapter(Subscription.class, new SubscriptionGsonAdapter());
		builder.registerTypeAdapter(BatchResult.class, new BatchResultGsonAdapter());
		builder.registerTypeAdapterFactory(new GeometryAdapterFactory());
		builder.registerTypeAdapterFactory(new JtsAdapterFactory());
	}

	public static GeoValue getGeoValue(String json) {
		return GSON.fromJson(json, GeoValue.class);
	}

	public static Geometry<?> getGeojsonGeometry(String json) {
		return GSON.fromJson(json, Geometry.class);
	}

	public static String toJson(Object obj) {
		return GSON.toJson(obj);
	}

	public static SubscriptionRequest getSubscriptionRequest(String json) {
		return GSON.fromJson(json, SubscriptionRequest.class);
	}
}
