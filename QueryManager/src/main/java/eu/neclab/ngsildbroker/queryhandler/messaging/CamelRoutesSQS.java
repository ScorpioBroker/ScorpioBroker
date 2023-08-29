package eu.neclab.ngsildbroker.queryhandler.messaging;

import java.net.ConnectException;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.faulttolerance.Retry;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import io.netty.channel.EventLoopGroup;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
@IfBuildProfile("sqs")
@IfBuildProperty(name = "quarkus.application.name", stringValue = "query-manager")
public class CamelRoutesSQS extends RouteBuilder {

	@Inject
	CamelContext camelContext;

	@ConfigProperty(name = "mp.messaging.outgoing.registry.endpoint-uri")
	String registryEndpointOut;

	@ConfigProperty(name = "mp.messaging.incoming.registryretrieve.endpoint-uri")
	String registryEndpointIn;

	private EventLoopGroup executor;

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
		onException(ConnectException.class).maximumRedeliveries(3).delay(5000);

		from(registryEndpointOut).unmarshal().json(BaseRequest.class).to(registryEndpointIn);
	}

}