package eu.neclab.ngsildbroker.infoserver.controller;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.exceptions.HttpErrorResponseException;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;

@RestController
@RequestMapping("/health")
public class InfoServerController {
	private static final int QUERY_MANAGER = 0;
	private static final int ENTITY_MANAGER = 1;
	private static final int STORAGE_MANAGER = 2;
	private static final int SUBSCRIPTION_MANAGER = 3;
	private static final int REGISTRY_MANAGER = 4;
	private static final int HISTORY_MANAGER = 5;
	HashMap<String, String> headers = new HashMap<String, String>();
	HashMap<Integer, URI> microService2Uri = new HashMap<Integer, URI>();
	HashMap<Integer, Integer> microService2SuccessReply = new HashMap<Integer, Integer>();
	HashMap<Integer, Integer> microService2HttpMethod = new HashMap<Integer, Integer>();
	String dummyMessage = "{\r\n" + 
			"	\"id\": \"NOTANURI\",\r\n" + 
			"	\"type\": \"https://uri.fiware.org/ns/data-models#AirQualityObserved\"\r\n" + 
			"	\r\n" + 
			"}";

	{
		try {
			headers.put("Accept", AppConstants.NGB_APPLICATION_JSON);
			microService2Uri.put(QUERY_MANAGER, new URI("http://localhost:9090/ngsi-ld/v1/entities/"));
			microService2Uri.put(ENTITY_MANAGER, new URI("http://localhost:9090/ngsi-ld/v1/entities/"));
			microService2Uri.put(STORAGE_MANAGER, new URI("http://localhost:9090/scorpio/v1/info/")); 
			microService2Uri.put(SUBSCRIPTION_MANAGER, new URI("http://localhost:9090/ngsi-ld/v1/subscriptions/"));
			microService2Uri.put(REGISTRY_MANAGER, new URI("http://localhost:9090/ngsi-ld/v1/csourceRegistrations/"));
			microService2Uri.put(HISTORY_MANAGER, new URI("http://localhost:9090/ngsi-ld/v1/temporal/entities/"));

			microService2SuccessReply.put(QUERY_MANAGER, 400);
			microService2SuccessReply.put(ENTITY_MANAGER, 400);
			microService2SuccessReply.put(STORAGE_MANAGER, 200);
			microService2SuccessReply.put(SUBSCRIPTION_MANAGER, 200);
			microService2SuccessReply.put(REGISTRY_MANAGER, 400);
			microService2SuccessReply.put(HISTORY_MANAGER, 400);

			microService2HttpMethod.put(QUERY_MANAGER, 0);
			microService2HttpMethod.put(ENTITY_MANAGER, 1);
			microService2HttpMethod.put(STORAGE_MANAGER, 0);
			microService2HttpMethod.put(SUBSCRIPTION_MANAGER, 0);
			microService2HttpMethod.put(REGISTRY_MANAGER, 0);
			microService2HttpMethod.put(HISTORY_MANAGER, 0);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


	HttpUtils httpUtils = HttpUtils.getInstance(null);
		
	@PostConstruct
	private void setup() {
	}
	
	@GetMapping
	public ResponseEntity<Object> getHealth(HttpServletRequest request) {
		HashMap<String, Object> result = new HashMap<String, Object>();

		result.put("Status of Querymanager", getStatus(QUERY_MANAGER));
		result.put("Status of Entitymanager", getStatus(ENTITY_MANAGER));
		result.put("Status of Storagemanager", getStatus(STORAGE_MANAGER));
		result.put("Status of Subscriptionmanager", getStatus(SUBSCRIPTION_MANAGER));
		result.put("Status of Registrymanager", getStatus(REGISTRY_MANAGER));
		result.put("Status of Historymanager", getStatus(HISTORY_MANAGER));
		return ResponseEntity.status(HttpStatus.ACCEPTED).header("Content-Type", "application/json")
				.body(DataSerializer.toJson(result));
	}

	private String getStatus(int component) {
		URI uri = microService2Uri.get(component);
		Integer success = microService2SuccessReply.get(component);
		try {
			switch (microService2HttpMethod.get(component)) {
			case 0:
				httpUtils.doGet(uri, headers);
				return "Up and running";
			case 1:
				httpUtils.doPost(uri, dummyMessage, null);
				return "Up and running";
			default:
				return "Unable to determine status";
			}
			
		} catch (IOException e) {
			if(e instanceof HttpErrorResponseException) {
				HttpErrorResponseException httpError = (HttpErrorResponseException) e;
				if(httpError.getStatusCode() == success) {
					return "Up and running";
				}
			}
		}
		return "Not running";

		
	}


}
