package eu.neclab.ngsildbroker.queryhandler.controller;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.github.jsonldjava.core.JsonLdProcessor;

import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.QueryControllerFunctions;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;

@RestController
@RequestMapping("/ngsi-ld/v1")
public class QueryController {

	@Autowired
	private QueryService queryService;

	@Value("${scorpio.entity.default-limit:50}")
	private int defaultLimit;
	@Value("${scorpio.entity.max-limit:1000}")
	private int maxLimit;

	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	private String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	/**
	 * Method(GET) for multiple attributes separated by comma list
	 * 
	 * @param request
	 * @param entityId
	 * @param attrs
	 * @return
	 * @throws ResponseException
	 */
	@GetMapping(path = "/entities/{entityId}")
	public ResponseEntity<String> getEntity(HttpServletRequest request,
			@RequestParam(value = "attrs", required = false) List<String> attrs,
			@RequestParam(value = "options", required = false) List<String> options,
			@PathVariable("entityId") String entityId) throws ResponseException {
		return QueryControllerFunctions.getEntity(queryService, request, attrs, options, entityId, false, defaultLimit,
				maxLimit);
	}

	/**
	 * Method(GET) for fetching all entities by kafka and other geo query operation
	 * by database
	 * 
	 * @param request
	 * @param type
	 * @return ResponseEntity object
	 */
	@GetMapping("/entities")
	public ResponseEntity<String> queryForEntities(HttpServletRequest request,
			@RequestParam(value = "attrs", required = false) List<String> attrs,
			@RequestParam(value = "limit", required = false) Integer limit,
			@RequestParam(value = "offset", required = false) Integer offset,
			@RequestParam(value = "qtoken", required = false) String qToken,
			@RequestParam(name = "options", required = false) List<String> options,
			@RequestParam(name = "services", required = false) Boolean showServices,
			@RequestParam(value = "count", required = false, defaultValue = "false") boolean count) {
		return QueryControllerFunctions.queryForEntries(queryService, request, attrs, limit, offset, qToken, options,
				showServices, count, false, defaultLimit, maxLimit);
	}

	@GetMapping(path = "/types")
	public ResponseEntity<String> getAllTypes(HttpServletRequest request,
			@RequestParam(value = "details", required = false, defaultValue = "false") boolean details) {
		return QueryControllerFunctions.getAllTypes(queryService, request, details, false, defaultLimit, maxLimit);
	}

	@GetMapping(path = "/types/{entityType}")
	public ResponseEntity<String> getType(HttpServletRequest request, @PathVariable("entityType") String type,
			@RequestParam(value = "details", required = false, defaultValue = "false") boolean details) {
		return QueryControllerFunctions.getType(queryService, request, type, details, false, defaultLimit, maxLimit);
	}

	@GetMapping(path = "/attributes")
	public ResponseEntity<String> getAllAttribute(HttpServletRequest request,
			@RequestParam(value = "details", required = false, defaultValue = "false") boolean details) {

		return QueryControllerFunctions.getAllAttribute(queryService, request, details, false, defaultLimit, maxLimit);
	}

	@GetMapping(path = "/attributes/{attributes}")
	public ResponseEntity<String> getAttributes(HttpServletRequest request,
			@PathVariable("attributes") String attributes,
			@RequestParam(value = "details", required = false, defaultValue = "false") boolean details) {
		return QueryControllerFunctions.getAttributes(queryService, request, attributes, details, false, defaultLimit,
				maxLimit);
	}

}