package eu.neclab.ngsildbroker.historymanager.controller;

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

import eu.neclab.ngsildbroker.commons.tools.ControllerFunctions;
import eu.neclab.ngsildbroker.historymanager.service.HistoryService;

@RestController
@RequestMapping("/ngsi-ld/v1/temporal/entityOperations")
public class EntityOperationsHistoryController {
	
	@Autowired
	HistoryService entityService;

	@Value("${batchoperations.maxnumber.create:-1}")
	int maxCreateBatch;
	@Value("${batchoperations.maxnumber.update:-1}")
	int maxUpdateBatch;
	@Value("${batchoperations.maxnumber.upsert:-1}")
	int maxUpsertBatch;
	@Value("${batchoperations.maxnumber.delete:-1}")
	int maxDeleteBatch;
	@Value("${defaultLimit}")
	int defaultLimit = 50;

	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@PostMapping("/create")
	public ResponseEntity<byte[]> createMultiple(HttpServletRequest request, @RequestBody String payload) {
		return ControllerFunctions.createMultiple(entityService, request, payload, maxCreateBatch);
	}

	@PostMapping("/upsert")
	public ResponseEntity<byte[]> upsertMultiple(HttpServletRequest request, @RequestBody String payload,
			@RequestParam(required = false, name = "options") String options) {
		return ControllerFunctions.upsertMultiple(entityService, request, payload, options, maxCreateBatch);
	}

	@PostMapping("/update")
	public ResponseEntity<byte[]> updateMultiple(HttpServletRequest request, @RequestBody String payload,
			@RequestParam(required = false, name = "options") String options) {
		return ControllerFunctions.updateMultiple(entityService, request, payload, maxUpdateBatch, options);
	}

	@PostMapping("/delete")
	public ResponseEntity<byte[]> deleteMultiple(HttpServletRequest request, @RequestBody String payload) {
		return ControllerFunctions.deleteMultiple(entityService, request, payload);
	}
	
//	@PostMapping("/query")
//	public ResponseEntity<byte[]> postQuery(HttpServletRequest request, @RequestBody String payload,
//			@RequestParam(value = "limit", required = false) Integer limit,
//			@RequestParam(value = "offset", required = false) Integer offset,
//			@RequestParam(value = "qtoken", required = false) String qToken,
//			@RequestParam(name = "options", required = false) List<String> options,
//			@RequestParam(value = "count", required = false, defaultValue = "false") boolean count) {
//		return ControllerFunctions.postQuery(entityService, request, payload, limit, offset, qToken, options, count,
//				defaultLimit);
//	}


}
