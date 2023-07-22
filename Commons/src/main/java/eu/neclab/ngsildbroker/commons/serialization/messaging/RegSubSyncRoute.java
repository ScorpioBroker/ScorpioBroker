package eu.neclab.ngsildbroker.commons.serialization.messaging;

import java.io.IOException;
import java.net.ConnectException;
import java.time.temporal.ChronoUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.builder.RouteBuilder;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.faulttolerance.Retry;

import eu.neclab.ngsildbroker.commons.datatypes.AliveAnnouncement;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.SyncMessage;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
@UnlessBuildProfile(anyOf = "in-memory")
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

	@ConfigProperty(name = "mp.messaging.outgoing.subalive.endpoint-uri")
	String subAliveEndpointOut;

	@ConfigProperty(name = "mp.messaging.outgoing.regsubalive.endpoint-uri")
	String regSubAliveEndpointOut;

	@ConfigProperty(name = "mp.messaging.outgoing.subsync.endpoint-uri")
	String subSyncEndpointOut;

	@ConfigProperty(name = "mp.messaging.outgoing.regsubsync.endpoint-uri")
	String regSubSyncEndpointOut;

	@ConfigProperty(name = "mp.messaging.outgoing.entity.endpoint-uri")
	String entityEndpointOut;

	@ConfigProperty(name = "mp.messaging.outgoing.entitybatch.endpoint-uri")
	String batchEndpointOut;

	@ConfigProperty(name = "mp.messaging.outgoing.registry.endpoint-uri")
	String registryEndpointOut;

	@ConfigProperty(name = "mp.messaging.outgoing.isubs.endpoint-uri")
	String iSubsEndpointOut;

	@ConfigProperty(name = "mp.messaging.outgoing.inotification.endpoint-uri")
	String iNotificationEndpointOut;

	@ConfigProperty(name = "mp.messaging.incoming.subaliveretrieve.endpoint-uri")
	String subAliveEndpointIn;

	@ConfigProperty(name = "mp.messaging.incoming.regsubaliveretrieve.endpoint-uri")
	String regSubAliveEndpointIn;

	@ConfigProperty(name = "mp.messaging.incoming.subsyncretrieve.endpoint-uri")
	String subSyncEndpointIn;

	@ConfigProperty(name = "mp.messaging.incoming.regsubsyncretrieve.endpoint-uri")
	String regSubSyncEndpointIn;

	@ConfigProperty(name = "mp.messaging.incoming.entityretrieve.endpoint-uri")
	String entityEndpointIn;

	@ConfigProperty(name = "mp.messaging.incoming.entitybatchretrieve.endpoint-uri")
	String batchEndpointIn;

	@ConfigProperty(name = "mp.messaging.incoming.registryretrieve.endpoint-uri")
	String registryEndpointIn;

	@ConfigProperty(name = "mp.messaging.incoming.isubsretrieve.endpoint-uri")
	String iSubsEndpointIn;

	@ConfigProperty(name = "mp.messaging.incoming.inotificationretrieve.endpoint-uri")
	String iNotificationEndpointIn;

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
		onException(ConnectException.class).maximumRedeliveries(3).delay(5000);

		from(regSubAliveEndpointOut).onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000)
				.to("log:retry").handled(true).end().unmarshal().json(AliveAnnouncement.class).to(regSubAliveEndpointIn)
				.onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000).to("log:retry")
				.handled(true).end();
		from(subAliveEndpointOut).onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000)
				.to("log:retry").handled(true).end().unmarshal().json(AliveAnnouncement.class).to(subAliveEndpointIn)
				.onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000).to("log:retry")
				.handled(true).end();
		from(regSubSyncEndpointOut).onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000)
				.to("log:retry").handled(true).end().unmarshal().json(SyncMessage.class).to(regSubSyncEndpointIn)
				.onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000).to("log:retry")
				.handled(true).end();
		from(subSyncEndpointOut).onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000)
				.to("log:retry").handled(true).end().unmarshal().json(SyncMessage.class).to(subSyncEndpointIn)
				.onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000).to("log:retry")
				.handled(true).end();
		from(entityEndpointOut).onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000)
				.to("log:retry").handled(true).end().unmarshal().json(BaseRequest.class).to(entityEndpointIn)
				.onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000).to("log:retry")
				.handled(true).end();
		from(batchEndpointOut).onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000)
				.to("log:retry").handled(true).end().unmarshal().json(BatchRequest.class).to(batchEndpointIn)
				.onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000).to("log:retry")
				.handled(true).end();
		from(registryEndpointOut).onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000)
				.to("log:retry").handled(true).end().unmarshal().json(BaseRequest.class).to(registryEndpointIn)
				.onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000).to("log:retry")
				.handled(true).end();
		from(iSubsEndpointOut).onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000)
				.to("log:retry").handled(true).end().unmarshal().json(SubscriptionRequest.class).to(iSubsEndpointIn)
				.onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000).to("log:retry")
				.handled(true).end();
		from(iNotificationEndpointOut).onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000)
				.to("log:retry").handled(true).end().unmarshal().json(InternalNotification.class)
				.to(iNotificationEndpointIn).onException(Exception.class).maximumRedeliveries(5)
				.maximumRedeliveryDelay(5000).to("log:retry").handled(true).end();

	}

}
