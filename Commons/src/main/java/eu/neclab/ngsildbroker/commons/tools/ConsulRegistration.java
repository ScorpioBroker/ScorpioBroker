package eu.neclab.ngsildbroker.commons.tools;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.runtime.Startup;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import io.vertx.ext.consul.CheckOptions;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.ServiceOptions;

@Startup
@Singleton
public class ConsulRegistration {

	Logger logger = LoggerFactory.getLogger(ConsulRegistration.class);

	@Inject
	Vertx vertx;

	private ConsulClient client;

	@ConfigProperty(name = "quarkus.application.name")
	String name;

	@ConfigProperty(name = "quarkus.http.port")
	int port;

	@ConfigProperty(name = "scorpio.consul.ervice-id", defaultValue = "")
	Optional<String> serviceId;

	// @ConfigProperty(name = "scorpio.consul.active", defaultValue = "false")
	boolean active = true;
	@ConfigProperty(name = "scorpio.consul.host", defaultValue = "localhost")
	String consulHost;

	@ConfigProperty(name = "scorpio.consul.port", defaultValue = "8500")
	int consulPort;

	@ConfigProperty(name = "scorpio.consul.timeout", defaultValue = "60")
	long consulTimeout;

	@ConfigProperty(name = "scorpio.consul.aclToken")
	Optional<String> consulAclToken;

	@ConfigProperty(name = "scorpio.consul.dc")
	Optional<String> consulDc;

	ServiceOptions serviceOptions;

	@PostConstruct
	void setup() throws UnknownHostException {
		logger.info("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
		if (active) {
			ConsulClientOptions options = new ConsulClientOptions().setHost(consulHost).setPort(consulPort)
					.setTimeout(consulTimeout);
			if (consulAclToken.isPresent()) {
				options = options.setAclToken(consulAclToken.get());
			}
			if (consulDc.isPresent()) {
				options = options.setDc(consulDc.get());
			}

			if (serviceId.isEmpty()) {
				serviceId = Optional.of(name + UUID.randomUUID().getLeastSignificantBits());
			}
			serviceOptions = new ServiceOptions().setName(name).setId(serviceId.get())
					.setCheckOptions(new CheckOptions().setInterval("30s").setHttp("/q/health/live")).setPort(port)
					.setAddress(InetAddress.getLocalHost().getHostName());

			this.client = ConsulClient.create(vertx, options);
			
			logger.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
			client.registerService(serviceOptions, event -> {
				logger.info("event called");
				logger.info(event.toString());
				if (event.failed()) {
					logger.info("BLAAAAA", event.cause());
				}
			});
		}
	}

	@PreDestroy
	void exit() {
		if (active) {
			client.deregisterService(serviceId.get());
		}
	}

}
