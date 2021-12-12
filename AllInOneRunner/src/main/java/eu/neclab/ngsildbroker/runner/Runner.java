package eu.neclab.ngsildbroker.runner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import eu.neclab.ngsildbroker.entityhandler.EntityHandler;
import eu.neclab.ngsildbroker.historymanager.HistoryHandler;
import eu.neclab.ngsildbroker.queryhandler.QueryHandler;
import eu.neclab.ngsildbroker.registryhandler.RegistryHandler;
import eu.neclab.ngsildbroker.subscriptionmanager.SubscriptionHandler;

@SpringBootApplication
public class Runner {

	public static void main(String[] args) throws Exception {
//		SpringApplication.run(new Class[] { AtContextServer.class, HistoryHandler.class, QueryHandler.class,
//				RegistryHandler.class, SubscriptionHandler.class, EntityHandler.class }, args);
		SpringApplication.run(new Class[] { RegistryHandler.class, HistoryHandler.class, QueryHandler.class,
				SubscriptionHandler.class, EntityHandler.class }, args);
	}

}
