package eu.neclab.ngsildbroker.runner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.nativex.hint.NativeHint;
import org.springframework.nativex.hint.ResourceHint;

import eu.neclab.ngsildbroker.entityhandler.EntityHandler;
import eu.neclab.ngsildbroker.historymanager.HistoryHandler;
import eu.neclab.ngsildbroker.queryhandler.QueryHandler;
import eu.neclab.ngsildbroker.registryhandler.RegistryHandler;
import eu.neclab.ngsildbroker.subscriptionmanager.SubscriptionHandler;

@SpringBootApplication
@NativeHint(options = { "--enable-all-security-services" }, resources = {
		@ResourceHint(patterns = "eu/neclab/ngsildbroker/entityhandler", isBundle = true),
		@ResourceHint(patterns = "org/flywaydb/core/internal/version.txt") })
public class Runner {

	public static void main(String[] args) throws Exception {
		System.out.println("LOOOK AT THIS");
		SpringApplication.run(new Class[] { RegistryHandler.class, HistoryHandler.class, QueryHandler.class,
				SubscriptionHandler.class, EntityHandler.class }, args);
	}

}
