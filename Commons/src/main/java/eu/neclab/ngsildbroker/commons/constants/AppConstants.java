package eu.neclab.ngsildbroker.commons.constants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @version 1.0
 * @date 12-Jul-2018
 */

public class AppConstants {

	// entities URL for
	public final static String ENTITES_URL = "/ngsi-ld/v1/entities/";
	public final static int ENTITIES_URL_ID = 0;
	// csource URL
	public final static String CSOURCE_URL = "/ngsi-ld/v1/csourceRegistrations/";
	public final static int CSOURCE_URL_ID = 1;
	// history
	public final static String HISTORY_URL = "/ngsi-ld/v1/temporal/entities/";
	public final static int HISTORY_URL_ID = 2;
	// subscriptions
	public final static String SUBSCRIPTIONS_URL = "/ngsi-ld/v1/subscriptions/";
	public final static int SUBSCRIPTIONS_URL_ID = 3;
	public static final int BATCH_URL_ID = 4;
	public final static int INTERNAL_CALL_ID = 5;

	public final static String NGB_APPLICATION_JSON = "application/json";
	public final static String NGB_APPLICATION_NQUADS = "application/n-quads";
	public final static String NGB_APPLICATION_JSONLD = "application/ld+json";
	public final static String NGB_APPLICATION_GENERIC = "application/*";
	public final static String NGB_GENERIC_GENERIC = "*/*";

	// allowed geometry types in queries params.
	public final static List<String> NGB_ALLOWED_GEOM_LIST = new ArrayList<String>(Arrays.asList("POINT", "POLYGON"));

	public final static byte[] NULL_BYTES = "null".getBytes();
	public static final String CORE_CONTEXT_URL_SUFFIX = "ngsi-ld-core-context";

	// constants for swagger
	public final static String SWAGGER_WEBSITE_LINK = "https://github.com/ScorpioBroker/ScorpioBroker";
	public final static String SWAGGER_CONTACT_LINK = "https://github.com/ScorpioBroker/ScorpioBroker";
	public static final int OPERATION_CREATE_ENTITY = 0;
	public static final int OPERATION_UPDATE_ENTITY = 1;
	public static final int OPERATION_APPEND_ENTITY = 2;
	public static final int OPERATION_DELETE_ATTRIBUTE_ENTITY = 3;
	public static final int OPERATION_CREATE_HISTORY_ENTITY = 4;
	public static final int OPERATION_UPDATE_HISTORY_ENTITY = 5;
	public static final int OPERATION_APPEND_HISTORY_ENTITY = 6;
	public static final int OPERATION_DELETE_ATTRIBUTE_HISTORY_ENTITY = 7;
	public static final int OPERATION_DELETE_ENTITY = 8;
	public static final String REQUEST_KV = "kv";
	public static final String REQUEST_WA = "withAttrs";
	public static final String REQUEST_WOA = "withoutAtts";
	public static final String REQUEST_T = "type";
	public static final String REQUEST_ID = "id";
	public static final String REQUEST_OV = "ov";
	public static final String REQUEST_HD = "headers";
	public static final String REQUEST_CSOURCE = "CSource";
	public static final String INTERNAL_NULL_KEY = ")$%^&";

	public static final String HTTP_METHOD_PATCH = "patch";
	public static final String NGB_APPLICATION_JSON_PATCH = "application/merge-patch+json";
	public static final int ENTITY_CREATE_PAYLOAD = 0;
	public static final int ENTITY_UPDATE_PAYLOAD = 1;
	public static final int ENTITY_RETRIEVED_PAYLOAD = 2;
	public static final int SUBSCRIPTION_CREATE_PAYLOAD = 3;
	public static final int SUBSCRIPTION_UPDATE_PAYLOAD = 10;
	public static final int CSOURCE_REG_CREATE_PAYLOAD = 4;
	public static final int CSOURCE_REG_UPDATE_PAYLOAD = 5;
	public static final int TEMP_ENTITY_CREATE_PAYLOAD = 6;
	public static final int TEMP_ENTITY_UPDATE_PAYLOAD = 7;
	public static final int TEMP_ENTITY_RETRIEVED_PAYLOAD = 8;
	public static final int ENTITY_ATTRS_UPDATE_PAYLOAD = 9;
	public static final Pattern DATE_TIME_MATCHER = Pattern.compile(
			"\\d\\d\\d\\d-(0[1-9]|1[0-2])-(0[0-9]|[1-2][0-9]|3[0-1])T([0-1][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\\.\\d{1,6})?Z");
	public static final List<String> FORCE_ARRAY_FIELDS = Arrays.asList(NGSIConstants.NGSI_LD_WATCHED_ATTRIBUTES,
			NGSIConstants.NGSI_LD_ENTITIES, NGSIConstants.NGSI_LD_PROPERTIES, NGSIConstants.NGSI_LD_RELATIONSHIPS,
			NGSIConstants.NGSI_LD_INFORMATION, NGSIConstants.NGSI_LD_RECEIVERINFO, NGSIConstants.NGSI_LD_NOTIFIERINFO,
			NGSIConstants.NGSI_LD_UPDATED, NGSIConstants.NGSI_LD_NOT_UPDATED);
	public static final int ATTRIBUTE_PAYLOAD = 11;
	public static final int NOTIFICAITION_RECEIVED = 12;
	public static final int QUERY_PAYLOAD = 13;
	public static final int QUERY_ENDPOINT = 0;
	public static final int HISTORY_ENDPOINT = 1;
	public static final int REGISTRY_ENDPOINT = 2;
	public static final int SUBSCRIPTION_ENDPOINT = 3;
	public static final int NOTIFICATION_ENDPOINT = 4;
	public static final String CSOURCE_SUBSCRIPTIONS_URL = "/ngsi-ld/v1/csourceSubscriptions/";
	public static final int CREATE_REQUEST = 0;
	public static final int APPEND_REQUEST = 1;
	public static final int UPDATE_REQUEST = 2;
	public static final int DELETE_REQUEST = 3;
	public static final int DELETE_ATTRIBUTE_REQUEST = 4;
	public static final String NGB_APPLICATION_GEO_JSON = "application/geo+json";
	public static final String INTERNAL_REGISTRATION_ID = "scorpio:hosted:registryentries";
	public static final String CONTENT_TYPE = "content-type";
	public static final int BATCH_ERROR_REQUEST = 99;


}