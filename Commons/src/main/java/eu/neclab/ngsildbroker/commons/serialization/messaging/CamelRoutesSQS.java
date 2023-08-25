package eu.neclab.ngsildbroker.commons.serialization.messaging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.builder.RouteBuilder;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.faulttolerance.Retry;

import eu.neclab.ngsildbroker.commons.datatypes.AliveAnnouncement;
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
@IfBuildProfile("sqs")
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

	@ConfigProperty(name = "entitybatch.endpoint-uri")
	String batchSQSEndpoint;

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
	@Retry(retryOn = ConnectException.class, delay = 2000)
	public void configure() {
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

		from("batchEndpointOut").split().method(MySplitter.class, "splitBySize")
				.setHeader("CamelSplitSize", simple("${property.CamelSplitSize}"))
				.setHeader("CamelSplitIndex", simple("${property.CamelSplitIndex}"))
				.setHeader("CamelSplitComplete", simple("${property.CamelSplitComplete}")).to("batchSQSEndpoint");

		from(batchSQSEndpoint).onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000)
				.to("log:retry").handled(true).end().aggregate(header("JMSCorrelationID"), new MyAggregationStrategy())
				.completionPredicate(header("CamelSplitComplete").isEqualTo(true)).end().unmarshal().json(BatchRequest.class)
				.to(batchEndpointIn).onException(Exception.class).maximumRedeliveries(5).maximumRedeliveryDelay(5000)
				.to("log:retry").handled(true).end();
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

	private class MySplitter {

		public static final int CHUNK_SIZE = 256 * 1024; // 256 KB

		public Iterator<byte[]> splitBySize(Exchange exchange) {
			byte[] data = exchange.getIn().getBody(byte[].class);
			int size = data.length;
			int chunks = (int) Math.ceil((double) size / CHUNK_SIZE);
			List<byte[]> result = new ArrayList<>(chunks);
			for (int i = 0; i < chunks; i++) {
				int from = i * CHUNK_SIZE;
				int to = Math.min(from + CHUNK_SIZE, size);
				byte[] chunk = Arrays.copyOfRange(data, from, to);
				result.add(chunk);
			}

			exchange.setProperty("CamelSplitSize", chunks);
			exchange.setProperty("CamelSplitIndex", 0);
			exchange.setProperty("CamelSplitComplete", false);
			return result.iterator();
		}
	}

	private class MyAggregationStrategy implements AggregationStrategy {

		@Override
		public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
			if (oldExchange == null) {
				// first time so create a new output stream to store the data
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				newExchange.getIn().setBody(bos);
				return newExchange;
			}

			// append the data from the new chunk to the existing output stream
			ByteArrayOutputStream bos = oldExchange.getIn().getBody(ByteArrayOutputStream.class);
			byte[] data = newExchange.getIn().getBody(byte[].class);
			try {
				bos.write(data);
			} catch (IOException e) {
				throw new RuntimeCamelException(e);
			}

			// check for completion
			boolean complete = newExchange.getIn().getHeader("CamelSplitComplete", Boolean.class);
			if (complete) {
				// set the output stream as the new body and close it
				newExchange.getIn().setBody(bos.toByteArray());
				try {
					bos.close();
				} catch (IOException e) {
					throw new RuntimeCamelException(e);
				}
			} else {
				// keep using the output stream as the body
				newExchange.getIn().setBody(bos);
			}

			return newExchange;
		}
	}
}