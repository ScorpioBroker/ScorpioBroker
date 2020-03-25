package eu.neclab.ngsildbroker.commons.tools;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.shared.Application;

public class MicroServiceUtils {
	private final static Logger logger = LogManager.getLogger(MicroServiceUtils.class);
	
	public static String getResourceURL(EurekaClient eurekaClient, String resource) {
		logger.trace("getSubscriptionResourceURL() :: started");
		Application application = eurekaClient.getApplication("gateway");
		InstanceInfo instanceInfo = application.getInstances().get(0);
		// TODO : search for a better way to resolve http or https
		String hostIP = instanceInfo.getIPAddr();
		int port = instanceInfo.getPort();
		StringBuilder url = new StringBuilder("http://").append(hostIP).append(":").append(port)
				.append(resource);
		// System.out.println("URL : "+url.toString());
		logger.trace("getSubscriptionResourceURL() :: completed");
		return url.toString();
	}
	
	public static URI getGatewayURL(EurekaClient eurekaClient) {
		logger.trace("getGatewayURL() :: started");
		Application application = eurekaClient.getApplication("gateway");
		InstanceInfo instanceInfo = application.getInstances().get(0);
		// TODO : search for a better way to resolve http or https
		String hostIP = instanceInfo.getIPAddr();
		int port = instanceInfo.getPort();
		StringBuilder url = new StringBuilder("http://").append(hostIP).append(":").append(port);
		// System.out.println("URL : "+url.toString());
		logger.trace("getGatewayURL() :: completed");
		try {
			return new URI(url.toString());
		} catch (URISyntaxException e) {
			throw new AssertionError("something went really wrong here when creating a URL... this should never happen but did with " + url.toString());
		}
	}

}
