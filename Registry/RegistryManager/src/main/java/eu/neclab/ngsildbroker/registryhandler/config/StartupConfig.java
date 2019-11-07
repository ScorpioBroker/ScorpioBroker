package eu.neclab.ngsildbroker.registryhandler.config;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.discovery.EurekaClient;

import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.Information;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;

@Component
public class StartupConfig {

	@Autowired
	@Qualifier("rmrestTemplate")
	RestTemplate restTemplate;
	@Autowired
	EurekaClient eurekaClient;
	@Value("${broker.id:#{null}}")
	String id;
	@Value("${broker.geoCoverage:#{null}}")
	String geom;
	@Value("${broker.parent.location.url:SELF}")
	String parentUrl;
	@Value("${broker.customEndpoint:}")
	String customEndpoint;
	@Autowired
	KafkaOps operations;
	@Value("${csource.source.topic}")
	String CSOURCE_TOPIC;
	@Autowired
	ObjectMapper objectMapper;
	// String s="\"type\": \"Polygon\",\"coordinates\": [[[100.0, 0.0],[101.0,
	// 0.0],[101.0, 1.0],[100.0, 1.0],[100.0, 0.0] ] ]";

	private final static Logger logger = LoggerFactory.getLogger(StartupConfig.class);

	@PostConstruct
	public void init() {

		logger.info("registering broker with parent :: " + parentUrl);
		// abort registration in case of fedration broker (SELF)
		if ("SELF".equalsIgnoreCase(parentUrl)) {
			logger.info("Parent Broker settings detected abort registration.");
			return;
		}
		if (parentUrl == null || geom == null) {
			logger.error("registration with parent falied : no endpoint and geom specified ");
			return;
		}
		String endpoint;
		if (!customEndpoint.isEmpty()) {
			endpoint = customEndpoint;
		} else {
			// TODO this has to be changed because the registry manager just straight up
			// crashes if the gateway is not up yet
			endpoint = MicroServiceUtils.getResourceURL(eurekaClient, "");
		}

		try {
			// set headers
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);
			String payload = getPayload(endpoint);
			logger.info("payload ::" + payload);
			HttpEntity<String> entity = new HttpEntity<String>(payload, headers);
			// call
			restTemplate.postForObject(new URI(parentUrl), entity, String.class);
			logger.info("Broker registered with parent at :" + parentUrl);
		} catch (HttpClientErrorException | HttpServerErrorException httpClientOrServerExc) {
			logger.error("status code::" + httpClientOrServerExc.getStatusCode());
			logger.error("Message::" + httpClientOrServerExc.getMessage());
			if (HttpStatus.INTERNAL_SERVER_ERROR.equals(httpClientOrServerExc.getStatusCode())) {
				logger.error("Broker registration failed due to parent broker.");
			}
			if (HttpStatus.CONFLICT.equals(httpClientOrServerExc.getStatusCode())) {
				logger.error("Broker already registered with parent.");
			}
		} catch (Exception e) {
			logger.error("Registration with parent failed::", e);
		}

	}

	// minimum payload for csource registration
	private String getPayload(String endpoint) throws URISyntaxException {
		// @formatter:off
		return "{\r\n" + "	\"id\": \"" + id + "\",\r\n" + "	\"type\": \"ContextSourceRegistration\",\r\n"
				+ "	\"information\": " + getCSInformationNode() + ",\r\n" + "	\"endpoint\": \"" + endpoint + "\",\r\n"
				+ "	\"location\": \"" + geom + "\",\r\n" + "	\"timestamp\": {\r\n" + "		\"start\": \""
				+ LocalDateTime.now() + "\"\r\n" + "	}\r\n" + "}";
		// @formatter:on
	}

	private String getCSInformationNode() throws URISyntaxException {
		Map<String, byte[]> records = operations.pullFromKafka(this.CSOURCE_TOPIC);
		// @formatter:off
		Map<String, String> streamRecords = records.entrySet().stream()
				.collect(Collectors.toMap(Map.Entry::getKey, e -> new String(e.getValue())));
		// @formatter:on
		return this.getInformationNode(streamRecords);
	}

	private String getInformationNode(Map<String, String> records) throws URISyntaxException {
		logger.trace("getCSourceRegistrationFromJson() :: started");
		List<Information> information = new ArrayList<Information>();
		for (String s : records.keySet()) {
			CSourceRegistration cSource = DataSerializer.getCSourceRegistration(records.get(s));
			information.addAll(cSource.getInformation());
		}
		String informationString = DataSerializer.toJson(information);
		return informationString;
	}
}
