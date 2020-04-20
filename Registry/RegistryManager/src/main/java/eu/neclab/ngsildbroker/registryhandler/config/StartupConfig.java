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
import org.springframework.stereotype.Service;
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
import eu.neclab.ngsildbroker.registryhandler.repository.CSourceDAO;

@Service
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
	@Value("${broker.reginfo:#{null}}")
	String reginfo;
	@Value("${broker.parent.location.url:SELF}")
	String parentUrl;
	@Value("${broker.customEndpoint:#{null}}")
	String customEndpoint;
	@Value("${broker.regOnlyLocal:#{false}}")
	boolean localOnlyAutoReg;
	@Autowired
	KafkaOps operations;
	@Value("${csource.source.topic}")
	String CSOURCE_TOPIC;
	@Autowired
	ObjectMapper objectMapper;
	@Autowired
	CSourceDAO cSourceDAO;
	boolean registered = false;
	private String currentRegistration = null;
	// String s="\"type\": \"Polygon\",\"coordinates\": [[[100.0, 0.0],[101.0,
	// 0.0],[101.0, 1.0],[100.0, 1.0],[100.0, 0.0] ] ]";

	private final static Logger logger = LoggerFactory.getLogger(StartupConfig.class);

	@PostConstruct
	public void init() {

		logger.debug("registering broker with parent :: " + parentUrl);
		// abort registration in case of fedration broker (SELF)
		if ("SELF".equalsIgnoreCase(parentUrl)) {
			logger.debug("Parent Broker settings detected abort registration.");
			return;
		}
		if (parentUrl == null || geom == null) {
			logger.error("registration with parent falied : no endpoint and geom specified ");
			return;
		}
		try {
			new URI(id);
		} catch (URISyntaxException e1) {
			logger.error("aborting registration. your id has to be a uri");
			return;
		}
		String endpoint;
		if (customEndpoint != null && !customEndpoint.isEmpty()) {
			logger.info("using custom endpoint " + customEndpoint);
			endpoint = customEndpoint;
		} else {
			// TODO this has to be changed because the registry manager just straight up
			// crashes if the gateway is not up yet
			try {
				endpoint = MicroServiceUtils.getResourceURL(eurekaClient, "");
			} catch (Exception e) {
				logger.error(
						"Failed to retrieve endpoint url. Please make sure that the gateway is running or provide a customEndpoint entry");
				return;
			}
		}
		if (!parentUrl.endsWith("/")) {
			parentUrl += "/";
		}
		URI parentUri;
		URI parentPatchUri;
		try {
			parentUri = new URI(parentUrl);
			parentPatchUri = new URI(parentUrl + id);
		} catch (URISyntaxException e1) {
			logger.error("your parentUrl is not a valid uri");
			return;
		}
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		String payload = getPayload(endpoint);
		HttpEntity<String> entity = new HttpEntity<String>(payload, headers);
		try {
			// set headers
			logger.info("registering with fed broker " + parentUrl);
			logger.info("payload ::" + payload);

			// call
			restTemplate.postForObject(parentUri, entity, String.class);
			registered = true;
			logger.debug("Broker registered with parent at :" + parentUrl);
		} catch (HttpClientErrorException | HttpServerErrorException httpClientOrServerExc) {
			logger.error("status code::" + httpClientOrServerExc.getStatusCode());
			logger.error("Message::" + httpClientOrServerExc.getMessage());
			if (HttpStatus.INTERNAL_SERVER_ERROR.equals(httpClientOrServerExc.getStatusCode())) {
				logger.error("Broker registration failed due to parent broker.");
			}
			if (HttpStatus.CONFLICT.equals(httpClientOrServerExc.getStatusCode())) {
				logger.debug("Broker already registered with parent. Attempting patch");
				try {
					restTemplate.patchForObject(parentPatchUri, entity, String.class);
				} catch (Exception e) {
					logger.error("patching failed");
				}
			}
		} catch (Exception e) {
			logger.error("failed to register with parent completly", e);
		}

	}

	// minimum payload for csource registration
	private String getPayload(String endpoint) {
		// @formatter:off
		return "{\r\n" + "	\"id\": \"" + id + "\",\r\n" + "	\"type\": \"ContextSourceRegistration\",\r\n"
				+ "	\"information\": " + getCSInformationNode() + ",\r\n" + "	\"endpoint\": \"" + endpoint + "\",\r\n"
				+ "	\"location\": \"" + geom + "\",\r\n" + "	\"timestamp\": {\r\n" + "		\"start\": \""
				+ LocalDateTime.now() + "\"\r\n" + "	}\r\n" + "}";
		// @formatter:on
	}
	
	public void handleUpdatedTypesForFed() {
		if(!registered) {
			return;
		}
		// don't know a better way. wait a moment for the database to actually change.
		try {
			Thread.sleep(200);
		} catch (InterruptedException e) {
			// unchanged intentional
			e.printStackTrace();
		}
		String current = this.currentRegistration;
		if (!current.equals(getCSInformationNode())) {
			String payload = "{\"information\": " + getCSInformationNode() + "}";
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<String> entity = new HttpEntity<String>(payload, headers);
			try {
				URI parentPatchUri = new URI(parentUrl + id);
				logger.debug("payload ::" + payload);
				// call
				restTemplate.patchForObject(parentPatchUri, entity, String.class);
				logger.debug("Broker registered with parent at :" + parentUrl);
			} catch (HttpClientErrorException | HttpServerErrorException httpClientOrServerExc) {
				logger.error("status code::" + httpClientOrServerExc.getStatusCode());
				logger.error("Message::" + httpClientOrServerExc.getMessage());
			} catch (Exception e) {
				logger.error("failed to update registery with parent completly", e.getMessage());
			}

		}
	}

	private String getCSInformationNode() {
		String resultString = null;
		if (reginfo != null) {
			return reginfo;
		}
		List<String> types;
		if (localOnlyAutoReg) {
			types = cSourceDAO.getLocalTypes();
		} else {
			types = cSourceDAO.getAllTypes();
		}
		if (types==null || types.isEmpty()) {
			resultString = "[]";
		} else {
			StringBuilder result = new StringBuilder("[{\"entities\": [");

			for (String type : types) {
				result.append("{\"type\": \"" + type + "\"},");
			}
			result.deleteCharAt(result.length() - 1);
			result.append("]}]");
			resultString = result.toString();
		}
		this.currentRegistration = resultString;
		return resultString;

		//
		// Map<String, byte[]> records = operations.pullFromKafka(this.CSOURCE_TOPIC);
		// // @formatter:off
		// Map<String, String> streamRecords = records.entrySet().stream()
		// .collect(Collectors.toMap(Map.Entry::getKey, e -> new String(e.getValue())));
		// // @formatter:on
		// return this.getInformationNode(streamRecords);
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
