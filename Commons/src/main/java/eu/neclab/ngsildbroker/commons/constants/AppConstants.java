package eu.neclab.ngsildbroker.commons.constants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.github.jsonldjava.core.JsonLdOptions;
import com.google.common.collect.Maps;

import eu.neclab.ngsildbroker.commons.datatypes.QueryRemoteHost;

/**
 * @version 1.0
 * @date 12-Jul-2018
 */

public class AppConstants {
	public static final String REG_MODE_KEY = "!@#$%";
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
	public static final int MERGE_PATCH_PAYLOAD = 17;
	public static final int REPLACE_ENTITY_PAYLOAD = 22;
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
			NGSIConstants.NGSI_LD_UPDATED, NGSIConstants.NGSI_LD_NOT_UPDATED, NGSIConstants.NGSI_LD_ATTRIBUTE_TYPES,
			NGSIConstants.NGSI_LD_TYPE_NAMES, NGSIConstants.NGSI_LD_TYPE_LIST);
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
	public static final int MERGE_PATCH_REQUEST = 17;
	public static final int REPLACE_ENTITY_REQUEST = 22;
	public static final int PARTIAL_UPDATE_REQUEST = 21;
	public static final int DELETE_REQUEST = 3;
	public static final int DELETE_ATTRIBUTE_REQUEST = 4;
	public static final int CREATE_TEMPORAL_REQUEST = 5;
	public static final int APPEND_TEMPORAL_REQUEST = 6;
	public static final int UPDATE_TEMPORAL_INSTANCE_REQUEST = 7;
	public static final int DELETE_TEMPORAL_REQUEST = 8;
	public static final int DELETE_TEMPORAL_ATTRIBUTE_REQUEST = 9;
	public static final int DELETE_TEMPORAL_ATTRIBUTE_INSTANCE_REQUEST = 10;
	public static final int UPSERT_REQUEST = 11;
	
	

	public static final int OPERATION_CREATE_REGISTRATION = 11;
	public static final int OPERATION_UPDATE_REGISTRATION = 12;
	public static final int OPERATION_DELETE_REGISTRATION = 13;

	public static final int DELETE_SUBSCRIPTION_REQUEST = 14;
	public static final int UPDATE_SUBSCRIPTION_REQUEST = 15;
	public static final int CREATE_SUBSCRIPTION_REQUEST = 16;

	public static final int REPLACE_ATTRIBUTE_REQUEST = 23;
	
	public static final int BATCH_CREATE_REQUEST = 30;
	public static final int BATCH_UPSERT_REQUEST = 31;
	public static final int BATCH_UPDATE_REQUEST = 32;
	public static final int BATCH_DELETE_REQUEST = 33;
	public static final int BATCH_MERGE_REQUEST = 34;

	public static final String NGB_APPLICATION_GEO_JSON = "application/geo+json";
	public static final String INTERNAL_TYPE_REGISTRATION_ID = "scorpio:hosted:types";
	public static final String INTERNAL_ATTRS_REGISTRATION_ID = "scorpio:hosted:attrs";
	public static final String INTERNAL_TYPE_ATTRS_REGISTRATION_ID = "scorpio:hosted:typeattrs";
	public static final String INTERNAL_ID_REGISTRATION_ID = "scorpio:hosted:ids";
	public static final String INTERNAL_FULL_REGISTRATION_ID = "scorpio:hosted:full";
	public static final String REGISTRY_CHANNEL = "registry";
	public static final String REGISTRY_RETRIEVE_CHANNEL = "registryretrieve";
	public static final String ENTITY_CHANNEL = "entity";
	public static final String ENTITY_RETRIEVE_CHANNEL = "entityretrieve";
	public static final String ENTITY_BATCH_CHANNEL = "entitybatch";
	public static final String ENTITY_BATCH_RETRIEVE_CHANNEL = "entitybatchretrieve";
	public static final String INTERNAL_SUBS_CHANNEL = "isubs";
	public static final String INTERNAL_RETRIEVE_SUBS_CHANNEL = "isubsretrieve";
	public static final String INTERNAL_NOTIFICATION_CHANNEL = "inotification";
	public static final String INTERNAL_RETRIEVE_NOTIFICATION_CHANNEL = "inotificationretrieve";
	public static final String SUB_SYNC_CHANNEL = "subsync";
	public static final String SUB_SYNC_RETRIEVE_CHANNEL = "subsyncretrieve";
	public static final String SUB_ALIVE_CHANNEL = "subalive";
	public static final String SUB_ALIVE_RETRIEVE_CHANNEL = "subaliveretrieve";
	public static final String HISTORY_CHANNEL = "history";
	public static final int BATCH_ERROR_REQUEST = 99;
	public static final String SQL_ALREADY_EXISTS = "23505";
	public static final String SQL_NOT_FOUND = "02000";
	public static final String SQL_INVALID_OPERATOR = "42804";
	public static final String SQL_INVALID_INTERVAL = "22007";

	public static final String NGB_APPLICATION_ZIP = "application/zip";
	public static final int INTERNAL_NOTIFICATION_REQUEST = 99;
	public static final int INTERVAL_NOTIFICATION_REQUEST = 100;
	public static final String ATTRIBUTE_LIST_PREFIX = "urn:ngsi-ld:AttributeList:";
	public static final String TYPE_LIST_PREFIX = "urn:ngsi-ld:EntityTypeList:";
	public static final String CONTENT_TYPE = "content-type";
	public static final String HIST_SYNC_RETRIEVE_CHANNEL = "histsyncretrieve";
	public static final String HIST_SYNC_CHANNEL = "histsync";
	public static final String INVALID_REGULAR_EXPRESSION = "2201B";
	public static final String INVALID_GEO_QUERY = "XX000";

	public static JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	public static final QueryRemoteHost DB_REMOTE_HOST = new QueryRemoteHost(INTERNAL_NULL_KEY, null, null, null, true,
			true, true, -1, null, null, false, false, null, null);
	public static final Map<String, QueryRemoteHost> DEFAULT_REMOTE_HOST_MAP = Maps
			.newHashMap(Map.of(INTERNAL_NULL_KEY, DB_REMOTE_HOST));
	
	public static final String TENANT_SERIALIZATION_CHAR = "a";
	public static final String PAYLOAD_SERIALIZATION_CHAR = "b";
	public static final String PREVPAYLOAD_SERIALIZATION_CHAR = "c";
	public static final String REQUESTTYPE_SERIALIZATION_CHAR = "d";
	public static final String SENDTIMESTAMP_SERIALIZATION_CHAR = "e";
	public static final String IDS_SERIALIZATION_CHAR = "f";
	public static final String ATTRIBNAME_SERIALIZATION_CHAR = "g";
	public static final String DATASETID_SERIALIZATION_CHAR = "h";
	public static final String DELETEALL_SERIALIZATION_CHAR = "i";
	public static final String DISTRIBUTED_SERIALIZATION_CHAR = "j";
	public static final String NOOVERWRITE_SERIALIZATION_CHAR = "k";
	public static final String INSTANCEID_SERIALIZATION_CHAR = "l";
	public static final String ZIPPED_SERIALIZATION_CHAR = "m";



}