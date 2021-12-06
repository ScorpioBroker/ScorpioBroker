package eu.neclab.ngsildbroker.commons.tools;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;

public class MicroServiceUtils {
	private final static Logger logger = LogManager.getLogger(MicroServiceUtils.class);

	@Value("${gatewayurl:}")
	private static String gatewayUrl;
	/*
	 * public static String getResourceURL(String resource) {
	 * 
	 * logger.trace("getSubscriptionResourceURL() :: started"); Application
	 * application = eurekaClient.getApplication("gateway"); InstanceInfo
	 * instanceInfo = application.getInstances().get(0); // TODO : search for a
	 * better way to resolve http or https String hostIP =
	 * "";//instanceInfo.getIPAddr(); int port = 1234;//instanceInfo.getPort();
	 * StringBuilder url = new
	 * StringBuilder("http://").append(hostIP).append(":").append(port)
	 * .append(resource); // System.out.println("URL : "+url.toString());
	 * logger.trace("getSubscriptionResourceURL() :: completed"); return
	 * url.toString(); }
	 */

	public static URI getGatewayURL() {
		logger.trace("getGatewayURL() :: started");
		String url = null;
		try {
			if (gatewayUrl == null || gatewayUrl.strip().isEmpty()) {
				String hostIP = InetAddress.getLocalHost().getHostName();
				int port = 9090;
				url = new StringBuilder("http://").append(hostIP).append(":").append(port).toString();
			} else {
				url = gatewayUrl;
			}
			logger.trace("getGatewayURL() :: completed");

			return new URI(url.toString());
		} catch (URISyntaxException | UnknownHostException e) {
			throw new AssertionError(
					"something went really wrong here when creating a URL... this should never happen but did with "
							+ url,
					e);
		}
	}

}
