package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.Map;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:40:03
 */
public interface ConfigurationHandler {

	/**
	 * 
	 * @param bundleId
	 * @param config
	 */
	public void configure(String bundleId, Map<String, String> config);

}