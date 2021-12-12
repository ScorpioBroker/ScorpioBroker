package eu.neclab.ngsildbroker.runner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.nativex.hint.NativeHint;
import org.springframework.nativex.hint.ResourceHint;
import org.springframework.nativex.hint.TypeHint;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RestController;

import eu.neclab.ngsildbroker.entityhandler.EntityHandler;
import eu.neclab.ngsildbroker.historymanager.HistoryHandler;
import eu.neclab.ngsildbroker.queryhandler.QueryHandler;
import eu.neclab.ngsildbroker.registryhandler.RegistryHandler;
import eu.neclab.ngsildbroker.subscriptionmanager.SubscriptionHandler;

@SpringBootApplication
@NativeHint(options = {"--enable-all-security-services"},resources = {
		@ResourceHint(patterns = "eu/neclab/ngsildbroker/*/controller/*.java"),
		@ResourceHint(patterns = "org/flywaydb/core/internal/version.txt") }, types = {
				@TypeHint(types = { RestController.class, Service.class }) })

public class Runner {

	public static void main(String[] args) throws Exception {
//		SpringApplication.run(new Class[] { AtContextServer.class, HistoryHandler.class, QueryHandler.class,
//				RegistryHandler.class, SubscriptionHandler.class, EntityHandler.class }, args);
		SpringApplication.run(new Class[] { RegistryHandler.class, HistoryHandler.class, QueryHandler.class,
				SubscriptionHandler.class, EntityHandler.class }, args);
	}

}
