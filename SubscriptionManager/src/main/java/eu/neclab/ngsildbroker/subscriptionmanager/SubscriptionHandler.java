package eu.neclab.ngsildbroker.subscriptionmanager;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;

@QuarkusMain
//@Import({ WebSecurityConfiguration.class, MicroServiceUtils.class })
public class SubscriptionHandler {

	public static void main(String[] args) {
		System.setProperty("org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH", "true");
		Quarkus.run(args);

	}

}
