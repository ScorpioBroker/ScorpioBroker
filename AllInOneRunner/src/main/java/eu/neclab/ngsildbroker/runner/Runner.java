package eu.neclab.ngsildbroker.runner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.nativex.hint.NativeHint;
import org.springframework.nativex.hint.ResourceHint;
import org.springframework.security.web.firewall.DefaultHttpFirewall;
import org.springframework.security.web.firewall.HttpFirewall;

import eu.neclab.ngsildbroker.entityhandler.EntityHandler;
import eu.neclab.ngsildbroker.historymanager.HistoryHandler;
import eu.neclab.ngsildbroker.queryhandler.QueryHandler;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.RegistrySubscriptionHandler;
import eu.neclab.ngsildbroker.registryhandler.RegistryHandler;
import eu.neclab.ngsildbroker.subscriptionmanager.SubscriptionHandler;

@SpringBootApplication
public class Runner {

	public static void main(String[] args) throws Exception {
		System.setProperty("org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH", "true");
		SpringApplication.run(new Class[] { RegistryHandler.class, HistoryHandler.class, QueryHandler.class,
				RegistrySubscriptionHandler.class, EntityHandler.class, SubscriptionHandler.class }, args);
	}

}
