package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.SubscriptionManager;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;

@RestController
@RequestMapping("/remotenotify")
public class NotificationController {

	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@Autowired
	SubscriptionManager subscriptionManager;
	private JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	@RequestMapping(method = RequestMethod.POST, value = "/{id}")
	public void notify(HttpServletRequest req, @RequestBody String payload,
			@PathVariable(name = NGSIConstants.QUERY_PARAMETER_ID, required = false) String id) {
		try {
			subscriptionManager.remoteNotify(id, DataSerializer.getNotification(JsonUtils.toString(JsonLdProcessor
					.expand(HttpUtils.getAtContext(req), payload, opts, AppConstants.NOTIFICAITION_RECEIVED, true))));
		} catch (ResponseException e) {
			// TODO Error Handling
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
