package eu.neclab.ngsildbroker.commons.constants;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import java.util.Arrays;

public interface NGSIConstants {
	public final static String GEO_REL_TYPE = "type";
	public final static String GEO_REL_REL = "rel";
	public final static String GEO_REL_NEAR = "near";
	public final static String GEO_REL_WITHIN = "within";
	public final static String GEO_REL_CONTAINS = "contains";
	public final static String GEO_REL_INTERSECTS = "intersects";
	public final static String GEO_REL_EQUALS = "equals";
	public final static String GEO_REL_DISJOINT = "disjoint";
	public final static String GEO_REL_OVERLAPS = "overlaps";
	public final static String GEO_REL_MAX_DISTANCE = "maxDistance";
	public final static String GEO_REL_MIN_DISTANCE = "minDistance";

	public final static String GEO_TYPE_POINT = "Point";
	public final static String GEO_TYPE_POLYGON = "Polygon";
	public final static String GEO_TYPE_LINESTRING = "LineString";
	public final static String GEO_TYPE_MULTI_POLYGON = "MultiPolygon";

	public final static String TIME_REL_BEFORE = "before";
	public final static String TIME_REL_AFTER = "after";
	public final static String TIME_REL_BETWEEN = "between";

	public final static String CSOURCE_INFORMATION = "information";
	public final static String CSOURCE_DESCRIPTION = "description";
	public final static String CSOURCE_ENDPOINT = "endpoint";
	public final static String CSOURCE_EXPIRES = "expiresAt";
	public final static String CSOURCE_NAME = "name";
	public final static String CSOURCE_TYPE = "type";
	public final static String CSOURCE_TIMESTAMP = "timestamp";
	public final static String CSOURCE_COORDINATES = "coordinates";
	public final static String NGSI_LD_SUBSCRIPTION_NAME = "https://uri.etsi.org/ngsi-ld/subscriptionName";
	public final static String CSOURCEREGISTRATION_NAME = "registrationName";
	public final static String OBJECT = "object";
	public final static String JSON_LD_ID = "@id";
	public final static String JSON_LD_TYPE = "@type";
	public final static String JSON_LD_VALUE = "@value";
	public final static String JSON_LD_CONTEXT = "@context";
	public final static String NGSI_LD_DEFAULT_PREFIX = "https://uri.etsi.org/ngsi-ld/default-context/";
	public final static String NGSI_LD_RELATIONSHIP = "https://uri.etsi.org/ngsi-ld/Relationship";
	public final static String NGSI_LD_PROPERTY = "https://uri.etsi.org/ngsi-ld/Property";
	public final static String NGSI_LD_HAS_VALUE = "https://uri.etsi.org/ngsi-ld/hasValue";
	public final static String NGSI_LD_HAS_OBJECT = "https://uri.etsi.org/ngsi-ld/hasObject";
	public final static String NGSI_LD_COORDINATES = "https://purl.org/geojson/vocab#coordinates"; // "https://uri.etsi.org/ngsi-ld/coordinates";
	public final static String NGSI_LD_GEOPROPERTY = "https://uri.etsi.org/ngsi-ld/GeoProperty";
	public final static String NGSI_LD_GEOPROPERTY_SHORT = "GeoProperty";
	public final static String NGSI_LD_LOCATION = "https://uri.etsi.org/ngsi-ld/location";
	public final static String NGSI_LD_LOCATION_SHORT = "location";
	public final static String NGSI_LD_CREATED_AT = "https://uri.etsi.org/ngsi-ld/createdAt";
	public final static String NGSI_LD_MODIFIED_AT = "https://uri.etsi.org/ngsi-ld/modifiedAt";
	public final static String NGSI_LD_OBSERVED_AT = "https://uri.etsi.org/ngsi-ld/observedAt";
	public final static String NGSI_LD_OBSERVATION_SPACE = "https://uri.etsi.org/ngsi-ld/observationSpace";
	public final static String NGSI_LD_OPERATION_SPACE = "https://uri.etsi.org/ngsi-ld/operationSpace";
	public final static String NGSI_LD_ATTRIBUTES = "https://uri.etsi.org/ngsi-ld/attributes";
	public final static String NGSI_LD_DATE_TIME = "https://uri.etsi.org/ngsi-ld/DateTime";
	public final static String NGSI_LD_DATE = "https://uri.etsi.org/ngsi-ld/Date";
	public final static String NGSI_LD_INFORMATION = "https://uri.etsi.org/ngsi-ld/information";
	public final static String NGSI_LD_RELATIONSHIPS = "https://uri.etsi.org/ngsi-ld/relationshipNames";
	public final static String NGSI_LD_PROPERTIES = "https://uri.etsi.org/ngsi-ld/propertyNames";
	public final static String NGSI_LD_INSTANCE_ID = "https://uri.etsi.org/ngsi-ld/instanceId";

	public final static String NGSI_LD_ID_PATTERN = "https://uri.etsi.org/ngsi-ld/idPattern";
	public final static String NGSI_LD_ENTITIES = "https://uri.etsi.org/ngsi-ld/entities";
	public final static String NGSI_LD_GEOMETRY = "https://purl.org/geojson/vocab#geometry";
	public final static String NGSI_LD_GEO_QUERY = "https://uri.etsi.org/ngsi-ld/geoQ";
	public final static String NGSI_LD_ACCEPT = "https://uri.etsi.org/ngsi-ld/accept";
	public final static String NGSI_LD_URI = "https://uri.etsi.org/ngsi-ld/uri";
	public final static String NGSI_LD_ENDPOINT = "https://uri.etsi.org/ngsi-ld/endpoint";
	public final static String NGSI_LD_FORMAT = "https://uri.etsi.org/ngsi-ld/format";
	public final static String NGSI_LD_NOTIFICATION = "https://uri.etsi.org/ngsi-ld/notification";
	public final static String NGSI_LD_QUERY = "https://uri.etsi.org/ngsi-ld/q";
	public final static String NGSI_LD_WATCHED_ATTRIBUTES = "https://uri.etsi.org/ngsi-ld/watchedAttributes";
	public final static String NGSI_LD_WATCHED_ATTRIBUTES_SHORT = "watchedAttributes";
	public final static String NGSI_LD_ENTITIES_SHORT = "entities";
	public final static String NGSI_LD_ATTRIBUTES_SHORT = "attributes";
	public final static String NGSI_LD_NAME = "https://uri.etsi.org/ngsi-ld/name";
	public final static String NGSI_LD_THROTTLING = "https://uri.etsi.org/ngsi-ld/throttling";
	public final static String NGSI_LD_TIME_INTERVAL = "https://uri.etsi.org/ngsi-ld/timeInterval";
	public final static String NGSI_LD_EXPIRES = "https://uri.etsi.org/ngsi-ld/expiresAt";
	public final static String NGSI_LD_STATUS = "https://uri.etsi.org/ngsi-ld/status";
	public final static String NGSI_LD_DESCRIPTION = "http://purl.org/dc/terms/description";
	public final static String NGSI_LD_GEO_REL = "https://uri.etsi.org/ngsi-ld/georel";
	public final static String NGSI_LD_TIME_STAMP = "https://uri.etsi.org/ngsi-ld/default-context/timestamp";
	public final static String NGSI_LD_TIMESTAMP_START = "https://uri.etsi.org/ngsi-ld/startAt";
	public final static String NGSI_LD_TIMESTAMP_END = "https://uri.etsi.org/ngsi-ld/endAt";
	public final static String NGSI_LD_POLYOGN = "https://purl.org/geojson/vocab#Polygon";
	public final static String NGSI_LD_POINT = "https://purl.org/geojson/vocab#Point";
	public final static String NGSI_LD_LINESTRING = "https://purl.org/geojson/vocab#LineString";
	public final static String NGSI_LD_SUBSCRIPTION_ID = "https://uri.etsi.org/ngsi-ld/subscriptionId";
	public final static String NGSI_LD_NOTIFIED_AT = "https://uri.etsi.org/ngsi-ld/notifiedAt";
	public final static String NGSI_LD_DATA = "https://uri.etsi.org/ngsi-ld/data";
	public final static String NGSI_LD_INTERNAL = "https://uri.etsi.org/ngsi-ld/internal";
	public final static String NGSI_LD_CSOURCE_REGISTRATION = "https://uri.etsi.org/ngsi-ld/ContextSourceRegistration";
	public final static String NGSI_LD_CSOURCE_REGISTRATION_SHORT = "ContextSourceRegistration";
	public final static String NGSI_LD_SUBSCRIPTION = "https://uri.etsi.org/ngsi-ld/Subscription";
	public final static String NGSI_LD_SUBSCRIPTION_SHORT = "Subscription";
	public final static String NGSI_LD_LAST_NOTIFICATION = "https://uri.etsi.org/ngsi-ld/lastNotification";
	public final static String NGSI_LD_LAST_FAILURE = "https://uri.etsi.org/ngsi-ld/lastFailure ";
	public final static String NGSI_LD_LAST_SUCCESS = "https://uri.etsi.org/ngsi-ld/lastSuccess";
	public final static String NGSI_LD_TIMES_SEND = "https://uri.etsi.org/ngsi-ld/timesSent";
	public final static String NGSI_LD_UNIT_CODE = "https://uri.etsi.org/ngsi-ld/unitCode";
	public final static String NGSI_LD_DATA_SET_ID = "https://uri.etsi.org/ngsi-ld/datasetId";
	public final static String NGSI_LD_IS_ACTIVE = "https://uri.etsi.org/ngsi-ld/isActive";
	public final static String NGSI_LD_ENTITY_LIST = "https://uri.etsi.org/ngsi-ld/EntityTypeList";
	public final static String NGSI_LD_TYPE_LIST = "https://uri.etsi.org/ngsi-ld/typeList";
	public final static String NGSI_LD_ENTITY_TYPE = "https://uri.etsi.org/ngsi-ld/EntityType";
	public final static String NGSI_LD_TYPE_NAME = "https://uri.etsi.org/ngsi-ld/typeName";
	public final static String NGSI_LD_ATTRIBUTE_NAMES = "https://uri.etsi.org/ngsi-ld/attributeNames";
	public final static String NGSI_LD_ATTRIBUTE_NAME = "https://uri.etsi.org/ngsi-ld/attributeName";
	public final static String NGSI_LD_ENTITY_TYPE_INFO = "https://uri.etsi.org/ngsi-ld/EntityTypeInfo";
	public final static String NGSI_LD_ENTITY_COUNT = "https://uri.etsi.org/ngsi-ld/entityCount";
	public final static String NGSI_LD_ATTRIBUTE_DETAILS = "https://uri.etsi.org/ngsi-ld/attributeDetails";
	public final static String NGSI_LD_TENANT = "https://uri.etsi.org/ngsi-ld/tenant";
	public final static String NGSI_LD_ATTRIBUTE_TYPES = "https://uri.etsi.org/ngsi-ld/attributeTypes";
	public final static String NGSI_LD_ATTRIBUTE = "https://uri.etsi.org/ngsi-ld/Attribute";
	public final static String NGSI_LD_ATTRS = "https://uri.etsi.org/ngsi-ld/attrs";
	public final static String NGSI_LD_ATTRIBUTE_LIST_1 = "https://uri.etsi.org/ngsi-ld/AttributeList";
	public final static String NGSI_LD_ATTRIBUTE_LIST_2 = "https://uri.etsi.org/ngsi-ld/attributeList";
	public final static String NGSI_LD_TYPE_NAMES = "https://uri.etsi.org/ngsi-ld/typeNames";
	public final static String NGSI_LD_ATTRIBUTE_COUNT = "https://uri.etsi.org/ngsi-ld/attributeCount";
	public final static String NGSI_LD_TEMPORAL_QUERY = "https://uri.etsi.org/ngsi-ld/temporalQ";
	public final static String NGSI_LD_MANAGEMENTINTERVAL = "https://uri.etsi.org/ngsi-ld/managementInterval";
	public static final String NGSI_LD_GEOREL = "https://uri.etsi.org/ngsi-ld/georel";
	// IMPORTANT! DO NOT MESS UP THIS ORDER!!! ONLY APPEND ON THE END NEW STUFF
	public final static String[] NGSI_LD_PAYLOAD_KEYS = { JSON_LD_ID, JSON_LD_TYPE, JSON_LD_CONTEXT,
			NGSI_LD_DEFAULT_PREFIX, NGSI_LD_HAS_VALUE, NGSI_LD_HAS_OBJECT, JSON_LD_VALUE, NGSI_LD_LOCATION,
			NGSI_LD_CREATED_AT, NGSI_LD_MODIFIED_AT, NGSI_LD_OBSERVED_AT, NGSI_LD_OBSERVATION_SPACE,
			NGSI_LD_OPERATION_SPACE, NGSI_LD_ATTRIBUTES, NGSI_LD_INFORMATION, NGSI_LD_INSTANCE_ID, NGSI_LD_COORDINATES,
			NGSI_LD_ID_PATTERN, NGSI_LD_ENTITIES, NGSI_LD_GEOMETRY, NGSI_LD_GEO_QUERY, NGSI_LD_ACCEPT, NGSI_LD_URI,
			NGSI_LD_ENDPOINT, NGSI_LD_FORMAT, NGSI_LD_NOTIFICATION, NGSI_LD_QUERY, NGSI_LD_WATCHED_ATTRIBUTES,
			NGSI_LD_NAME, NGSI_LD_THROTTLING, NGSI_LD_TIME_INTERVAL, NGSI_LD_EXPIRES, NGSI_LD_STATUS,
			NGSI_LD_DESCRIPTION, NGSI_LD_GEO_REL, NGSI_LD_TIME_STAMP, NGSI_LD_TIMESTAMP_START, NGSI_LD_TIMESTAMP_END,
			NGSI_LD_SUBSCRIPTION_ID, NGSI_LD_NOTIFIED_AT, NGSI_LD_DATA, NGSI_LD_INTERNAL, NGSI_LD_LAST_NOTIFICATION,
			NGSI_LD_LAST_FAILURE, NGSI_LD_LAST_SUCCESS, NGSI_LD_TIMES_SEND, NGSI_LD_UNIT_CODE, NGSI_LD_DATA_SET_ID,
			NGSI_LD_MANAGEMENTINTERVAL };

	public final static String[] NGSI_LD_SUBSCRIPTON_PAYLOAD_KEYS = { JSON_LD_ID, JSON_LD_TYPE, JSON_LD_CONTEXT,
			NGSI_LD_ENTITIES, NGSI_LD_ID_PATTERN, NGSI_LD_GEO_QUERY, NGSI_LD_NOTIFICATION, NGSI_LD_ATTRIBUTES,
			NGSI_LD_ENDPOINT, NGSI_LD_ACCEPT, NGSI_LD_URI, NGSI_LD_FORMAT, NGSI_LD_QUERY, NGSI_LD_WATCHED_ATTRIBUTES,
			NGSI_LD_TIMES_SEND, NGSI_LD_THROTTLING, NGSI_LD_TIME_INTERVAL, NGSI_LD_EXPIRES, NGSI_LD_STATUS,
			NGSI_LD_DESCRIPTION, NGSI_LD_IS_ACTIVE, NGSI_LD_TIMESTAMP_END, NGSI_LD_TIMESTAMP_START,
			NGSI_LD_SUBSCRIPTION_NAME };

	public final static String GEO_JSON_COORDINATES = "coordinates";
	public final static String GEO_JSON_TYPE = "type";

	public final static String VALUE = "value";

	// Entity validation attribute types
	public final static String VALID_NGSI_ATTRIBUTE_TYPES = "Relationship,Property,DateTime";

	// url decode format
	public final static String ENCODE_FORMAT = "UTF-8";
	// query parameter url
	public final static String QUERY_URL = "entities/?";

	// query parameter
	public final static String QUERY_PARAMETER_TYPE = "type";
	public final static String QUERY_PARAMETER_ID = "id";
	public final static String QUERY_PARAMETER_IDPATTERN = "idPattern";
	public final static String QUERY_PARAMETER_ATTRS = "attrs";
	public final static String QUERY_PARAMETER_QUERY = "q";
	public final static String QUERY_PARAMETER_GEOREL = "georel";
	public final static String QUERY_PARAMETER_GEOMETRY = "geometry";
	public final static String QUERY_PARAMETER_COORDINATES = "coordinates";
	public final static String QUERY_PARAMETER_GEOPROPERTY = "geoproperty";
	public final static String QUERY_PARAMETER_TIMEREL = "timerel";
	public final static String QUERY_PARAMETER_OFFSET = "offset";
	public final static String QUERY_PARAMETER_LIMIT = "limit";
	public final static String QUERY_PARAMETER_QTOKEN = "qtoken";
	public final static String QUERY_PARAMETER_TIME = "timeAt";
	public final static String QUERY_PARAMETER_ENDTIME = "endTimeAt";
	public final static String QUERY_PARAMETER_DETAILS = "details";
	public final static String QUERY_PARAMETER_TIMEPROPERTY = "timeproperty";
	public final static String QUERY_PARAMETER_LOCATION = "location";
	public final static String QUERY_PARAMETER_CREATED_AT = "createdAt";
	public final static String QUERY_PARAMETER_MODIFIED_AT = "modifiedAt";
	public final static String QUERY_PARAMETER_OBSERVED_AT = "observedAt";
	public final static String QUERY_PARAMETER_UNIT_CODE = "unitCode";
	public final static String QUERY_PARAMETER_DATA_SET_ID = "datasetId";
	public final static String QUERY_PARAMETER_OBSERVATION_SPACE = "observationspace";
	public final static String QUERY_PARAMETER_OPERATION_SPACE = "operationspace";
	public final static String QUERY_PARAMETER_GEOREL_DISTANCE = "distance";
	public final static String QUERY_PARAMETER_DEFAULT_GEOPROPERTY = NGSIConstants.NGSI_LD_LOCATION;
	public final static String QUERY_PARAMETER_DEFAULT_TIMEPROPERTY = NGSIConstants.NGSI_LD_OBSERVED_AT;
	public final static String QUERY_PARAMETER_OPTIONS = "options";
	public final static String QUERY_PARAMETER_OPTIONS_SYSATTRS = "sysAttrs";
	public final static String QUERY_PARAMETER_OPTIONS_KEYVALUES = "keyValues";
	public final static String QUERY_PARAMETER_OPTIONS_COMPRESS = "compress";
	public final static String QUERY_PARAMETER_OPTIONS_TEMPORALVALUES = "temporalValues";

	public final static String QUERY_EQUAL = "==";
	public final static String QUERY_UNEQUAL = "!=";
	public final static String QUERY_GREATEREQ = ">=";
	public final static String QUERY_GREATER = ">";
	public final static String QUERY_LESSEQ = "<=";
	public final static String QUERY_LESS = "<";
	public final static String QUERY_PATTERNOP = "~=";
	public final static String QUERY_NOTPATTERNOP = "!~=";
	// public final static String CHECK_QUERY_STRING_URI = "/";

	public static final String ALLOWED_IN_DEFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]'Z'";
	public static final String ALLOWED_OUT_DEFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
	public static final String ALLOWED_OUT_DEFAULT_DATE_FORMAT_NOTIFIEDAT = "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]'Z'";
	public static final String DEFAULT_FORGIVING_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
	public static final String HEADER_REL_LDCONTEXT = "http://www.w3.org/ns/json-ld#context";

	public static final HashMap<Integer, String> HTTP_CODE_2_NGSI_ERROR = new HashMap<Integer, String>();
	public static final List<String> ALLOWED_GEOMETRIES = Arrays.asList("Point", "MultiPoint", "LineString",
			"MultiLineString", "Polygon", "MultiPolygon");
	public static final List<String> ALLOWED_GEOREL = Arrays.asList("near", "equals", "disjoint", "intersects",
			"within", "contains", "overlaps");
	public static final List<String> SPECIAL_PROPERTIES = Arrays.asList(NGSI_LD_CREATED_AT, NGSI_LD_OBSERVED_AT,
			NGSI_LD_MODIFIED_AT, NGSI_LD_DATA_SET_ID, NGSI_LD_UNIT_CODE, JSON_LD_ID, JSON_LD_TYPE, JSON_LD_CONTEXT);
	public static final String MQTT_QOS = "mqtt_qos";
	public static final String MQTT_VERSION = "mqtt_version";
	public static final String DEFAULT_DATA_SET_ID = "https://uri.etsi.org/ngsi-ld/default-data-set-id";
	public static final String NGSI_LD_ENDPOINT_REGEX = ".*\\\"https\\:\\/\\/uri\\.etsi\\.org\\/ngsi-ld\\/endpoint\\\"\\W*\\:\\W*\\[\\W*\\{\\W*\\@value\\\"\\:\\W*\\\"(http(s)*\\:\\/\\/\\S*)\\\".*";
	public static final String NGSI_LD_ENDPOINT_TENANT = ".*\\\"https\\:\\/\\/uri\\.etsi\\.org\\/ngsi-ld\\/default-context/tenant\\\"\\W*\\:\\W*\\[\\W*\\{\\W*\\@value\\\"\\:\\W*\\\"(\\S*)\\\".*";
	public static final String NGSI_LD_FORBIDDEN_KEY_CHARS_REGEX = "([\\<\\\"\\'\\=\\;\\(\\)\\>\\?\\*])";
	public static final String NGSI_LD_FORBIDDEN_KEY_CHARS = "<,\",',=,;,(,),>,?,*";
	public static final String[] VALID_SUB_ENDPOINT_SCHEMAS = { "http", "https", "mqtt", "mqtts" };
	public final static String QUERY_PARAMETER_DELETE_ALL = "deleteAll";
	public static final String NGSI_LD_NOTIFIERINFO = "https://uri.etsi.org/ngsi-ld/default-context/notifierinfo";
	public static final String NGSI_LD_RECEIVERINFO = "https://uri.etsi.org/ngsi-ld/default-context/receiverInfo";
	public static final String NGSI_LD_MQTT_QOS = "https://uri.etsi.org/ngsi-ld/default-context/qos";
	public static final String NGSI_LD_MQTT_VERSION = "https://uri.etsi.org/ngsi-ld/default-context/version";
	public static final Integer DEFAULT_MQTT_QOS = 0;
	public static final String DEFAULT_MQTT_VERSION = "mqtt5.0";
	public static final String CONTENT_TYPE = "contentType";
	public static final String ACCEPTED_LINK = "link";
	public static final String METADATA = "metadata";
	public static final String BODY = "body";
	public static final String MQTT_VERSION_5 = "mqtt5.0";
	public static final String MQTT_VERSION_3 = "mqtt3.1.1";
	public static final String[] VALID_MQTT_VERSION = { "mqtt5.0", "mqtt3.1.1" };
	public static final Integer[] VALID_QOS = { 0, 1, 2 };
	public static final String COUNT_HEADER_RESULT = "ngsild-results-count";
	public static final String REGEX_NGSI_LD_ATTR_TYPES = new String(
			NGSI_LD_PROPERTY + "|" + NGSI_LD_RELATIONSHIP + "|" + NGSI_LD_GEOPROPERTY);

	public final static String NOTIFICATION = "Notification";
	// DO NOT CHANGE THIS HEADERS ARE MADE TO LOWER CASE BY SPRING
	public static final String TENANT_HEADER_FOR_INTERNAL_CHECK = "ngsild-tenant";
	public static final String LINK_HEADER = "Link";
	public static final String QUERY_TYPE = "Query";
	public static final String ISACTIVE_FALSE = "paused";
	public static final String ISACTIVE_TRUE = "active";

	public static final Set<String> CORE_CONTEXT_URLS = Sets.newHashSet(
			"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.4.jsonld",
			"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld",
			"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld");
	public static final String NGSI_LD_END_TIME_AT = "https://uri.etsi.org/ngsi-ld/endTimeAt";
	public static final String NGSI_LD_TIME_AT = "https://uri.etsi.org/ngsi-ld/timeAt";
	public static final String NGSI_LD_TIME_POPERTY = "https://uri.etsi.org/ngsi-ld/timeproperty";
	public static final String NGSI_LD_TIME_REL = "https://uri.etsi.org/ngsi-ld/timerel";
	public static final String NGSI_LD_OBSERVATIONINTERVAL = "https://uri.etsi.org/ngsi-ld/observationInterval";
	public static final Set<String> LOCATIONS_IN_REGISTRATION = Set.of(NGSI_LD_LOCATION, NGSI_LD_OBSERVATION_SPACE,
			NGSI_LD_OPERATION_SPACE);
	public static final String NO_OVERWRITE_OPTION = "noOverwrite";
	public static final String OVERWRITE_OPTION = "overwrite";
	public static final String REPLACE_OPTION = "replace";
	public static final String UPDATE_OPTION = "update";
	public static final String CSOURCE_NOTIFICATION = "ContextSource Notification";
	public static final String QUERY_PARAMETER_LAST_N = "lastN";
	public static final String FEATURE_COLLECTION = "FeatureCollection";
	public static final String FEATURES = "features";
	public static final String FEATURE = "Feature";
	public static final String GEOMETRY = "geometry";
	public static final String PROPERTIES = "properties";
	public static final String QUERY_PARAMETER_GEOMETRY_PROPERTY = "geometryProperty";
	public static final String QUERY_PARAMETER_COUNT = "count";
	public static final String QUERY_PARAMETER_CSF = "csf";
	public static final String JSON_LD_LIST = "@list";
	public static final String TENANT_HEADER = "NGSILD-Tenant";
	public static final String NGSI_LD_REASON = "https://uri.etsi.org/ngsi-ld/reason";
	public static final String NGSI_LD_NOT_UPDATED = "https://uri.etsi.org/ngsi-ld/default-context/notUpdated";
	public static final String NGSI_LD_UPDATED = "https://uri.etsi.org/ngsi-ld/updated";
	public static final String NGSI_LD_UPDATED_SHORT = "updated";
	public static final String NGSI_LD_CONTEXT_SOURCE_INFO = "https://uri.etsi.org/ngsi-ld/default-context/contextSourceInfo";
	public static final String NGSI_LD_CSF = "https://uri.etsi.org/ngsi-ld/csf";
	public static final String NGSI_LD_TRIGGER_REASON = "https://uri.etsi.org/ngsi-ld/triggerReason";
	public static final String QUERY_PARAMETER_SCOPE_QUERY = "scopeQ";
	public static final String NGSI_LD_GEOPROPERTY_GEOQ_ATTRIB = "https://uri.etsi.org/ngsi-ld/geoproperty";
	public static final String NGSI_LD_SCOPE = "https://uri.etsi.org/ngsi-ld/default-context/scope";
	public static final String NGSI_LD_SCOPE_Q = "https://uri.etsi.org/ngsi-ld/default-context/scopeQ";
	public static final Object NGSI_LD_TENANT_SHORT = "tenant";
}