package eu.neclab.ngsildbroker.commons.tools;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

@Service
public class MicroServiceUtils {
	private final static Logger logger = LogManager.getLogger(MicroServiceUtils.class);

	@Autowired
	KafkaAdmin kafkaAdmin;

	@Value("${scorpio.topics.entity:}")
	private String entityTopic;
	@Value("${scorpio.topics.registry:}")
	private String registryTopic;
	@Value("${scorpio.topics.temporal:}")
	private String temporalTopic;
	@Value("${scorpio.topics.internalnotification:}")
	private String internalnotificationTopic;
	@Value("${scorpio.topics.internalregsub:}")
	private String internalregsubTopic;

	@Value("${scorpio.topics.partitions:-1}")
	private int partitions;

	@Value("${scorpio.topics.replication:-1}")
	private short replication;

	@PostConstruct
	private void setupTopics() {
		kafkaAdmin.createOrModifyTopics(new NewTopic(entityTopic, partitions, replication));
		kafkaAdmin.createOrModifyTopics(new NewTopic(registryTopic, partitions, replication));
		kafkaAdmin.createOrModifyTopics(new NewTopic(temporalTopic, partitions, replication));
		kafkaAdmin.createOrModifyTopics(new NewTopic(internalnotificationTopic, partitions, replication));
		kafkaAdmin.createOrModifyTopics(new NewTopic(internalregsubTopic, partitions, replication));
	}

	@Value("${scorpio.gatewayurl:}")
	private String gatewayUrl;

	@Value("${server.port}")
	private int port;

	public URI getGatewayURL() {
		logger.trace("getGatewayURL() :: started");
		String url = null;
		try {
			if (gatewayUrl == null || gatewayUrl.strip().isEmpty()) {
				String hostIP = InetAddress.getLocalHost().getHostName();
				url = new StringBuilder("http://").append(hostIP).append(":").append(port).toString();
			} else {
				url = gatewayUrl;
			}
			logger.trace("getGatewayURL() :: completed");

			return new URI(url.toString());
		} catch (URISyntaxException | UnknownHostException e) {
			throw new AssertionError(
					"something went really wrong here when creating a URL... this should never happen but did with "
							+ url,
					e);
		}
	}

}
