package eu.neclab.ngsildbroker.commons.constants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @version 1.0
 * @date 12-Jul-2018
 */

public class AppConstants {

	// entities URL for
	public final static String ENTITES_URL = "/ngsi-ld/v1/entities/";
	// csource URL
	public final static String CSOURCE_URL = "/ngsi-ld/v1/csourceRegistrations/";
	//history
	public final static String HISTORY_URL="/ngsi-ld/v1/temporal/entities/";
	//subscriptions
	public final static String SUBSCRIPTIONS_URL="/ngsi-ld/v1/subscriptions/";
	
	public final static String NGB_APPLICATION_JSON="application/json";
	public final static String NGB_APPLICATION_JSONLD="application/ld+json";
	public final static String NGB_APPLICATION_GENERIC ="application/*";
	public final static String NGB_GENERIC_GENERIC ="*/*";
	
	//allowed geometry types in queries params.
	public final static List<String> NGB_ALLOWED_GEOM_LIST=new ArrayList<String>(Arrays.asList("POINT","POLYGON"));
	
	
	public final static byte[] NULL_BYTES = "null".getBytes();


}
