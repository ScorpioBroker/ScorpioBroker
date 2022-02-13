package eu.neclab.ngsildbroker.entityhandler.controller;

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
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;

@RestController
@RequestMapping("/ngsi-ld/v1/entityOperations")
public class EntityBatchController {

	@Autowired
	EntityService entityService;

	@Value("${batchoperations.maxnumber.create:-1}")
	int maxCreateBatch;
	@Value("${batchoperations.maxnumber.update:-1}")
	int maxUpdateBatch;
	@Value("${batchoperations.maxnumber.upsert:-1}")
	int maxUpsertBatch;
	@Value("${batchoperations.maxnumber.delete:-1}")
	int maxDeleteBatch;

	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@PostMapping("/create")
	public ResponseEntity<String> createMultiple(HttpServletRequest request, @RequestBody String payload) {
		return EntryControllerFunctions.createMultiple(entityService, request, payload, maxCreateBatch,
				AppConstants.ENTITY_CREATE_PAYLOAD);
	}

	@PostMapping("/upsert")
	public ResponseEntity<String> upsertMultiple(HttpServletRequest request, @RequestBody String payload,
			@RequestParam(required = false, name = "options") String options) {
		return EntryControllerFunctions.upsertMultiple(entityService, request, payload, options, maxCreateBatch,
				AppConstants.ENTITY_CREATE_PAYLOAD);
	}

	@PostMapping("/update")
	public ResponseEntity<String> updateMultiple(HttpServletRequest request, @RequestBody String payload,
			@RequestParam(required = false, name = "options") String options) {
		return EntryControllerFunctions.updateMultiple(entityService, request, payload, maxUpdateBatch, options,
				AppConstants.ENTITY_UPDATE_PAYLOAD);
	}

	@PostMapping("/delete")
	public ResponseEntity<String> deleteMultiple(HttpServletRequest request, @RequestBody String payload) {
		return EntryControllerFunctions.deleteMultiple(entityService, request, payload, AppConstants.ENTITY_CREATE_PAYLOAD);
	}

}
