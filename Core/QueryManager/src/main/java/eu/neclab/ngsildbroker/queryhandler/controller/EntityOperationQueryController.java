package eu.neclab.ngsildbroker.queryhandler.controller;

import java.util.List;
import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.github.jsonldjava.core.JsonLdProcessor;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.tools.ControllerFunctions;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;

@RestController
@RequestMapping("/ngsi-ld/v1/entityOperations")
public class EntityOperationQueryController {

	@Autowired
	QueryService queryService;

	@Value("${defaultLimit}")
	int defaultLimit = 50;

	@Value("${maxLimit}")
	int maxLimit = 1000;

	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@PostMapping("/query")
	public ResponseEntity<String> postQuery(HttpServletRequest request, @RequestBody String payload,
			@RequestParam(value = "limit", required = false) Integer limit,
			@RequestParam(value = "offset", required = false) Integer offset,
			@RequestParam(value = "qtoken", required = false) String qToken,
			@RequestParam(name = "options", required = false) List<String> options,
			@RequestParam(value = "count", required = false, defaultValue = "false") boolean count) {
		return ControllerFunctions.postQuery(queryService, request, payload, limit, offset, qToken, options, count,
				defaultLimit, AppConstants.QUERY_PAYLOAD);
	}
}
