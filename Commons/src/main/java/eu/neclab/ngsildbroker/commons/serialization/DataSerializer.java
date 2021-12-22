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
import eu.neclab.ngsildbroker.commons.datatypes.BaseRequest;
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
		
		

	}

	


	



	// ------------------------------------------------------------------------------------------

	public static BaseRequest getBaseRequest(String string) {
		// TODO Auto-generated method stub
		return null;
	}

	public static EntityRequest getEntityRequest(String message) {
		// TODO Auto-generated method stub
		return null;
	}

	public static String toJson(BatchResult result) {
		// TODO Auto-generated method stub
		return null;
	}

	public static String toJson(Subscription remoteSub) {
		// TODO Auto-generated method stub
		return null;
	}

	public static SubscriptionRequest getSubscriptionRequest(String subscriptionString) {
		// TODO Auto-generated method stub
		return null;
	}

	public static String toJson(EntityRequest request) {
		// TODO Auto-generated method stub
		return null;
	}

	public static String toJson(CSourceRequest request) {
		// TODO Auto-generated method stub
		return null;
	}

	public static String toJson(HistoryEntityRequest request) {
		// TODO Auto-generated method stub
		return null;
	}

	public static String toJson(List<Subscription> subscriptions) {
		// TODO Auto-generated method stub
		return null;
	}

	public static String toJson(SubscriptionRequest value) {
		// TODO Auto-generated method stub
		return null;
	}

	public static String toJson(QueryParams qp) {
		// TODO Auto-generated method stub
		return null;
	}

}
