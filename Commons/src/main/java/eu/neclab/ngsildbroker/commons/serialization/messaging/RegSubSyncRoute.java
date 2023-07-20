package eu.neclab.ngsildbroker.commons.serialization.messaging;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.builder.RouteBuilder;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import eu.neclab.ngsildbroker.commons.datatypes.AliveAnnouncement;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
public class RegSubSyncRoute extends RouteBuilder {
//	scorpio.topics.entity=ENTITY
//			scorpio.topics.entitybatch=ENTITYBATCH
//			scorpio.topics.registry=REGISTRY
//			scorpio.topics.temporal=TEMPORAL
//			scorpio.topics.internalnotification=I_NOTIFY
//			scorpio.topics.internalregsub=I_REGSUB
//			scorpio.topics.subalive=SUB_ALIVE
//			scorpio.topics.subsync=SUB_SYNC
//			scorpio.topics.regsubalive=REG_SUB_ALIVE
//			scorpio.topics.regsubsync=REG_SUB_SYNC
	@Inject
	CamelContext camelContext;

	// This is needed so that @postconstruct runs on the startup thread and not on a
	// worker thread later on
	void startup(@Observes StartupEvent event) {
	}

	@PostConstruct
	void setup() {
		camelContext.getGlobalOptions().put("CamelJacksonEnableTypeConverter", "true");
		camelContext.getGlobalOptions().put("CamelJacksonTypeConverterToPojo", "true");
	}

	@Override
	public void configure() throws Exception {
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
//		from("direct:sendMyObject").marshal().json().to("paho-mqtt5:REG_SUB_ALIVE");
//		from("direct:sendMyObject").marshal().json();
		from("paho-mqtt5:REG_SUB_ALIVE?brokerUrl=tcp://localhost:1883").unmarshal().json(AliveAnnouncement.class).to("reactive-streams:REG_SUB_ALIVE");
		from("paho-mqtt5:SUB_ALIVE?brokerUrl=tcp://localhost:1883").unmarshal().json(AliveAnnouncement.class).to("reactive-streams:SUB_ALIVE");

	}

}
