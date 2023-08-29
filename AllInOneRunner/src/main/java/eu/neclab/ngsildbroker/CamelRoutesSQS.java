package eu.neclab.ngsildbroker;

import java.net.ConnectException;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.faulttolerance.Retry;

import eu.neclab.ngsildbroker.commons.datatypes.AliveAnnouncement;
import eu.neclab.ngsildbroker.commons.datatypes.SyncMessage;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.InternalNotification;
import eu.neclab.ngsildbroker.commons.serialization.messaging.MyAggregationStrategy;
import eu.neclab.ngsildbroker.commons.serialization.messaging.MySplitter;
import eu.neclab.ngsildbroker.historyentitymanager.messaging.HistoryMessagingSQS;
import eu.neclab.ngsildbroker.subscriptionmanager.messaging.SubscriptionMessagingSQS;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.netty.channel.EventLoopGroup;
import io.quarkus.arc.Arc;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
@IfBuildProfile("sqs")
@IfBuildProperty(name = "quarkus.application.name", stringValue = "aio-runner")
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

	@ConfigProperty(name = "mp.messaging.outgoing.subalive.endpoint-uri")
	String subAliveEndpointOut;

	@ConfigProperty(name = "mp.messaging.outgoing.subsync.endpoint-uri")
	String subSyncEndpointOut;

	@ConfigProperty(name = "mp.messaging.outgoing.entity.endpoint-uri")
	String entityEndpointOut;

	@ConfigProperty(name = "entitybatch.endpoint-uri")
	String batchSQSEndpoint;

	@ConfigProperty(name = "mp.messaging.outgoing.registry.endpoint-uri")
	String registryEndpointOut;

	@ConfigProperty(name = "mp.messaging.outgoing.inotification.endpoint-uri")
	String iNotificationEndpointOut;

	@ConfigProperty(name = "mp.messaging.incoming.subaliveretrieve.endpoint-uri")
	String subAliveEndpointIn;

	@ConfigProperty(name = "mp.messaging.incoming.subsyncretrieve.endpoint-uri")
	String subSyncEndpointIn;

	@ConfigProperty(name = "mp.messaging.incoming.entityretrieve.endpoint-uri")
	String entityEndpointIn;

	@ConfigProperty(name = "mp.messaging.incoming.registryretrieve.endpoint-uri")
	String registryEndpointIn;

	@ConfigProperty(name = "mp.messaging.incoming.inotificationretrieve.endpoint-uri")
	String iNotificationEndpointIn;

	@ConfigProperty(name = "mp.messaging.outgoing.entitybatch.endpoint-uri")
	String batchEndpointOut;

//	@Inject
//	HistoryMessagingSQS historyMessagingSQS;
//	@Inject
//	SubscriptionMessagingSQS subscriptionMessagingSQS;

	@Inject
	Vertx vertx;

	private EventLoopGroup executor;
	
	this.executor = vertx.getDelegate().nettyEventLoopGroup();

	// This is needed so that @postconstruct runs on the startup thread and not on a
	// worker thread later on
	void startup(@Observes StartupEvent event) {
	}

	@PostConstruct
	void setup() {
		camelContext.getGlobalOptions().put("CamelJacksonEnableTypeConverter", "true");
		camelContext.getGlobalOptions().put("CamelJacksonTypeConverterToPojo", "true");
		this.executor = vertx.getDelegate().nettyEventLoopGroup();
	}

	@Override
	@Retry(retryOn = ConnectException.class, delay = 2000)
	public void configure() {
//		from("direct:sendMyObject").marshal().json().to("paho-mqtt5:REG_SUB_ALIVE");
//		from("direct:sendMyObject").marshal().json();
		//onException(ConnectException.class).maximumRedeliveries(3).delay(5000).end();

		from(subAliveEndpointOut).unmarshal().json(AliveAnnouncement.class).to(subAliveEndpointIn);

		from(subSyncEndpointOut).unmarshal().json(SyncMessage.class).to(subSyncEndpointIn);
		from(entityEndpointOut).unmarshal().json(BaseRequest.class).to(entityEndpointIn);
		from("direct:batch").split().method(MySplitter.class, "splitBySize")
				.setHeader("CorrelationID", simple("${exchangeProperty.CorrelationID}"))
				.setHeader("CamelSplitSize", simple("${exchangeProperty.CamelSplitSize}"))
				.setHeader("CamelSplitIndex", simple("${exchangeProperty.CamelSplitIndex}"))
				.setHeader("CamelSplitComplete", simple("${exchangeProperty.CamelSplitComplete}")).to(batchSQSEndpoint);

		from(batchSQSEndpoint).aggregate(simple("${header.CorrelationID}"), new MyAggregationStrategy())
				.completionPredicate(header("CamelSplitComplete").isEqualTo(true)).unmarshal().json(BatchRequest.class)
				.process(exchange -> {
					SubscriptionMessagingSQS subscriptionMessagingSQS = Arc.container()
							.instance(SubscriptionMessagingSQS.class).get();
					HistoryMessagingSQS historyMessagingSQS = Arc.container().instance(HistoryMessagingSQS.class).get();
					System.out.println("batch aggregate in sub " + exchange.getIn().getMessageId());
					Uni.combine().all()
							.unis(subscriptionMessagingSQS
									.baseHandleBatchEntities(exchange.getIn().getBody(BatchRequest.class)),
									historyMessagingSQS.baseHandleBatch(exchange.getIn().getBody(BatchRequest.class)))
							.asTuple().runSubscriptionOn(executor).subscribe();

				});

		from(registryEndpointOut).unmarshal().json(BaseRequest.class).to(registryEndpointIn);
		from(iNotificationEndpointOut).unmarshal().json(InternalNotification.class).to(iNotificationEndpointIn);

	}
}