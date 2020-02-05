package eu.neclab.ngsildbroker.commons.constants;

import java.util.HashMap;
import java.util.List;

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

	public final static String GEO_TYPE_POINT="Point";
	public final static String GEO_TYPE_POLYGON="Polygon";
	public final static String GEO_TYPE_LINESTRING="LineString";
	
	public final static String TIME_REL_BEFORE = "before";
	public final static String TIME_REL_AFTER = "after";
	public final static String TIME_REL_BETWEEN = "between";
	
	public final static String CSOURCE_INFORMATION = "information";
	public final static String CSOURCE_DESCRIPTION = "description";
	public final static String CSOURCE_ENDPOINT = "endpoint";
	public final static String CSOURCE_EXPIRES = "expires";
	public final static String CSOURCE_NAME = "name";
	public final static String CSOURCE_TYPE = "type";
	public final static String CSOURCE_TIMESTAMP = "timestamp";
	public final static String CSOURCE_COORDINATES="coordinates";

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
	public final static String NGSI_LD_COORDINATES = "https://uri.etsi.org/ngsi-ld/coordinates";
	public final static String NGSI_LD_GEOPROPERTY = "https://uri.etsi.org/ngsi-ld/GeoProperty";
	public final static String NGSI_LD_GEOPROPERTY_SHORT = "GeoProperty";
	public final static String NGSI_LD_LOCATION = "https://uri.etsi.org/ngsi-ld/location";
	public final static String NGSI_LD_CREATED_AT = "https://uri.etsi.org/ngsi-ld/createdAt";
	public final static String NGSI_LD_MODIFIED_AT = "https://uri.etsi.org/ngsi-ld/modifiedAt";
	public final static String NGSI_LD_OBSERVED_AT = "https://uri.etsi.org/ngsi-ld/observedAt";
	public final static String NGSI_LD_OBSERVATION_SPACE = "https://uri.etsi.org/ngsi-ld/observationSpace";
	public final static String NGSI_LD_OPERATION_SPACE = "https://uri.etsi.org/ngsi-ld/operationSpace";
	public final static String NGSI_LD_ATTRIBUTES = "https://uri.etsi.org/ngsi-ld/attributes";
	public final static String NGSI_LD_OBJECT = "https://uri.etsi.org/ngsi-ld/hasObject";
	public final static String NGSI_LD_DATE_TIME = "https://uri.etsi.org/ngsi-ld/DateTime";
	public final static String NGSI_LD_DATE = "https://uri.etsi.org/ngsi-ld/Date";
	public final static String NGSI_LD_TIME = "https://uri.etsi.org/ngsi-ld/Time";
	public final static String NGSI_LD_INFORMATION="https://uri.etsi.org/ngsi-ld/information";
	public final static String NGSI_LD_RELATIONSHIPS = "https://uri.etsi.org/ngsi-ld/relationships";
    public final static String NGSI_LD_PROPERTIES = "https://uri.etsi.org/ngsi-ld/properties";
    public final static String NGSI_LD_INSTANCE_ID = "https://uri.etsi.org/ngsi-ld/instanceId";
    
	public final static String NGSI_LD_ID_PATTERN = "https://uri.etsi.org/ngsi-ld/idPattern";
	public final static String NGSI_LD_ENTITIES = "https://uri.etsi.org/ngsi-ld/entities";
	public final static String NGSI_LD_GEOMETRY = "https://uri.etsi.org/ngsi-ld/geometry";
	public final static String NGSI_LD_GEO_QUERY = "https://uri.etsi.org/ngsi-ld/geoQ";
	public final static String NGSI_LD_ACCEPT = "https://uri.etsi.org/ngsi-ld/accept";
	public final static String NGSI_LD_URI = "https://uri.etsi.org/ngsi-ld/uri";
	public final static String NGSI_LD_ENDPOINT = "https://uri.etsi.org/ngsi-ld/endpoint";
	public final static String NGSI_LD_FORMAT = "https://uri.etsi.org/ngsi-ld/format";
	public final static String NGSI_LD_NOTIFICATION = "https://uri.etsi.org/ngsi-ld/notification";
	public final static String NGSI_LD_QUERY = "https://uri.etsi.org/ngsi-ld/q";
	public final static String NGSI_LD_WATCHED_ATTRIBUTES = "https://uri.etsi.org/ngsi-ld/watchedAttributes";
	public final static String NGSI_LD_THROTTLING = "https://uri.etsi.org/ngsi-ld/throttling";
	public final static String NGSI_LD_TIME_INTERVAL = "https://uri.etsi.org/ngsi-ld/timeInterval";
	public final static String NGSI_LD_EXPIRES = "https://uri.etsi.org/ngsi-ld/expires";
	public final static String NGSI_LD_STATUS = "https://uri.etsi.org/ngsi-ld/status";
	public final static String NGSI_LD_DESCRIPTION = "https://uri.etsi.org/ngsi-ld/description";
	public final static String NGSI_LD_GEO_REL = "https://uri.etsi.org/ngsi-ld/georel";
	public final static String NGSI_LD_TIME_STAMP="https://uri.etsi.org/ngsi-ld/timestamp";
	public final static String NGSI_LD_TIMESTAMP_START="https://uri.etsi.org/ngsi-ld/start";
	public final static String NGSI_LD_TIMESTAMP_END="https://uri.etsi.org/ngsi-ld/end";
	public final static String NGSI_LD_POLYOGN="https://uri.etsi.org/ngsi-ld/Polygon";
	public final static String NGSI_LD_POINT="https://uri.etsi.org/ngsi-ld/Point";
	public final static String NGSI_LD_LINESTRING="https://uri.etsi.org/ngsi-ld/LineString";
	public final static String NGSI_LD_SUBSCRIPTION_ID ="https://uri.etsi.org/ngsi-ld/subscriptionId";
	public final static String NGSI_LD_NOTIFIED_AT ="https://uri.etsi.org/ngsi-ld/notifiedAt";
	public final static String NGSI_LD_DATA ="https://uri.etsi.org/ngsi-ld/data";
	public final static String NGSI_LD_INTERNAL="https://uri.etsi.org/ngsi-ld/internal";
	
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
	public final static String QUERY_PARAMETER_TIME = "time";
	public final static String QUERY_PARAMETER_ENDTIME = "endTime";
	public final static String QUERY_PARAMETER_TIMEPROPERTY = "timeproperty";
	public final static String QUERY_PARAMETER_LOCATION = "location";
	public final static String QUERY_PARAMETER_CREATED_AT = "createdAt";
	public final static String QUERY_PARAMETER_MODIFIED_AT = "modifiedAt";
	public final static String QUERY_PARAMETER_OBSERVED_AT = "observedAt";
	public final static String QUERY_PARAMETER_UNIT_CODE = "unitCode";
	public final static String QUERY_PARAMETER_DATA_SET_ID  = "datasetId";
	public final static String QUERY_PARAMETER_OBSERVATION_SPACE = "observationspace";
	public final static String QUERY_PARAMETER_OPERATION_SPACE = "operationspace";
	public final static String QUERY_PARAMETER_GEOREL_DISTANCE = "distance";
	public final static String QUERY_PARAMETER_DEFAULT_GEOPROPERTY = NGSIConstants.NGSI_LD_LOCATION;
	public final static String QUERY_PARAMETER_DEFAULT_TIMEPROPERTY = NGSIConstants.NGSI_LD_OBSERVED_AT;
	public final static String QUERY_PARAMETER_OPTIONS = "options";
	public final static String QUERY_PARAMETER_OPTIONS_SYSATTRS = "sysAttrs";
	public final static String QUERY_PARAMETER_OPTIONS_KEYVALUES = "keyValues";
	public final static String QUERY_PARAMETER_OPTIONS_TEMPORALVALUES = "temporalValues";
	
	public final static String QUERY_EQUAL = "==";
	public final static String QUERY_UNEQUAL = "!=";
	public final static String QUERY_GREATEREQ = ">=";
	public final static String QUERY_GREATER = ">";
	public final static String QUERY_LESSEQ = "<=";
	public final static String QUERY_LESS = "<";
	public final static String QUERY_PATTERNOP = "~=";
	public final static String QUERY_NOTPATTERNOP = "!~=";
	//public final static String CHECK_QUERY_STRING_URI = "/";
		
	public static final String NGSI_LD_UNIT_CODE = "https://uri.etsi.org/ngsi-ld/unitCode";
	public static final String NGSI_LD_DATA_SET_ID = "https://uri.etsi.org/ngsi-ld/datasetId";
	
	public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]'Z'";
	public static final String DEFAULT_FORGIVING_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
	public static final String HEADER_REL_LDCONTEXT = "http://www.w3.org/ns/json-ld#context";
	
	public static final HashMap<Integer, String> HTTP_CODE_2_NGSI_ERROR = new HashMap<Integer, String>();
	public static final List<String> ALLOWED_GEOMETRIES = Arrays.asList("Point", "MultiPoint", "LineString","MultiLineString", "Polygon", "MultiPolygon");
	public static final List<String> ALLOWED_GEOREL = Arrays.asList("near","equals","disjoint","intersects","within","contains","overlaps");
	
}
