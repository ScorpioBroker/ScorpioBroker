package eu.neclab.ngsildbroker.historymanager.controller;

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
import eu.neclab.ngsildbroker.commons.controllers.EntryControllerFunctions;
import eu.neclab.ngsildbroker.commons.controllers.QueryControllerFunctions;
import eu.neclab.ngsildbroker.commons.interfaces.PayloadQueryParamParser;
import eu.neclab.ngsildbroker.historymanager.service.HistoryPostQueryParser;
import eu.neclab.ngsildbroker.historymanager.service.HistoryService;

@RestController
@RequestMapping("/ngsi-ld/v1/temporal/entityOperations")
public class EntityOperationsHistoryController {

	@Autowired
	private HistoryService entityService;

	@Value("${scorpio.history.batch-operations.create.max:-1}")
	private int maxCreateBatch;
	@Value("${scorpio.history.batch-operations.update.max:-1}")
	private int maxUpdateBatch;
	@Value("${scorpio.history.batch-operations.upsert.max:-1}")
	private int maxUpsertBatch;
	@Value("${scorpio.history.batch-operations.delete.max:-1}")
	private int maxDeleteBatch;
	@Value("${scorpio.history.default-limit:50}")
	private int defaultLimit;
	@Value("${scorpio.history.default-limit:1000}")
	private int maxLimit;

	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	private String coreContext;

	private PayloadQueryParamParser paramParser = new HistoryPostQueryParser();

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@PostMapping("/create")
	public ResponseEntity<String> createMultiple(HttpServletRequest request, @RequestBody String payload) {
		return EntryControllerFunctions.createMultiple(entityService, request, payload, maxCreateBatch,
				AppConstants.TEMP_ENTITY_CREATE_PAYLOAD);
	}

	@PostMapping("/upsert")
	public ResponseEntity<String> upsertMultiple(HttpServletRequest request, @RequestBody String payload,
			@RequestParam(required = false, name = "options") String options) {
		return EntryControllerFunctions.upsertMultiple(entityService, request, payload, options, maxCreateBatch,
				AppConstants.TEMP_ENTITY_CREATE_PAYLOAD);
	}

	@PostMapping("/update")
	public ResponseEntity<String> updateMultiple(HttpServletRequest request, @RequestBody String payload,
			@RequestParam(required = false, name = "options") String options) {
		return EntryControllerFunctions.updateMultiple(entityService, request, payload, maxUpdateBatch, options,
				AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD);
	}

	@PostMapping("/delete")
	public ResponseEntity<String> deleteMultiple(HttpServletRequest request, @RequestBody String payload) {
		return EntryControllerFunctions.deleteMultiple(entityService, request, payload,
				AppConstants.TEMP_ENTITY_CREATE_PAYLOAD);
	}

	@PostMapping("/query")
	public ResponseEntity<String> postQuery(HttpServletRequest request, @RequestBody String payload,
			@RequestParam(value = "limit", required = false) Integer limit,
			@RequestParam(value = "offset", required = false) Integer offset,
			@RequestParam(value = "qtoken", required = false) String qToken,
			@RequestParam(name = "options", required = false) List<String> options,
			@RequestParam(value = "count", required = false, defaultValue = "false") boolean count) {

		return QueryControllerFunctions.postQuery(entityService, request, payload, limit, offset, qToken, options,
				count, defaultLimit, maxLimit, AppConstants.QUERY_PAYLOAD, paramParser);
	}

}
