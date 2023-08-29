package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import java.net.ConnectException;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.faulttolerance.Retry;

import eu.neclab.ngsildbroker.commons.datatypes.AliveAnnouncement;
import eu.neclab.ngsildbroker.commons.datatypes.SyncMessage;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
@IfBuildProfile("sqs")
@IfBuildProperty(name = "quarkus.application.name", stringValue = "registry-subscription-manager")
public class CamelRoutesSQS extends RouteBuilder {
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

	@ConfigProperty(name = "mp.messaging.outgoing.regsubalive.endpoint-uri")
	String regSubAliveEndpointOut;

	@ConfigProperty(name = "mp.messaging.outgoing.regsubsync.endpoint-uri")
	String regSubSyncEndpointOut;

	@ConfigProperty(name = "mp.messaging.outgoing.registry.endpoint-uri")
	String registryEndpointOut;

	@ConfigProperty(name = "mp.messaging.outgoing.isubs.endpoint-uri")
	String iSubsEndpointOut;

	@ConfigProperty(name = "mp.messaging.incoming.regsubaliveretrieve.endpoint-uri")
	String regSubAliveEndpointIn;

	@ConfigProperty(name = "mp.messaging.incoming.regsubsyncretrieve.endpoint-uri")
	String regSubSyncEndpointIn;

	@ConfigProperty(name = "mp.messaging.incoming.registryretrieve.endpoint-uri")
	String registryEndpointIn;

	@ConfigProperty(name = "mp.messaging.incoming.isubsretrieve.endpoint-uri")
	String iSubsEndpointIn;

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
	@Retry(retryOn = ConnectException.class, delay = 2000)
	public void configure() {
//		from("direct:sendMyObject").marshal().json().to("paho-mqtt5:REG_SUB_ALIVE");
//		from("direct:sendMyObject").marshal().json();
		onException(ConnectException.class).maximumRedeliveries(3).delay(5000);

		from(regSubAliveEndpointOut).unmarshal().json(AliveAnnouncement.class).to(regSubAliveEndpointIn);
		from(regSubSyncEndpointOut).unmarshal().json(SyncMessage.class).to(regSubSyncEndpointIn);

		from(registryEndpointOut).unmarshal().json(BaseRequest.class).to(registryEndpointIn);
		from(iSubsEndpointOut).unmarshal().json(SubscriptionRequest.class).to(iSubsEndpointIn);

	}
}