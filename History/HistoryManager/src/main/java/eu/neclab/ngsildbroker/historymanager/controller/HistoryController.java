package eu.neclab.ngsildbroker.historymanager.controller;

import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.controllers.EntryControllerFunctions;
import eu.neclab.ngsildbroker.commons.controllers.QueryControllerFunctions;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.historymanager.repository.HistoryDAO;
import eu.neclab.ngsildbroker.historymanager.service.HistoryService;

@RestController
@RequestMapping("/ngsi-ld/v1/temporal/entities")
public class HistoryController {

	private final static Logger logger = LoggerFactory.getLogger(HistoryController.class);

	@Autowired
	HistoryDAO historyDAO;
	@Autowired
	HistoryService historyService;
	@Value("${atcontext.url}")
	String atContextServerUrl;
	@Value("${scorpio.history.defaultLimit:50}")
	int defaultLimit;
	@Value("${scorpio.history.maxLimit:1000}")
	int maxLimit;

	private JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@PostMapping
	public ResponseEntity<String> createTemporalEntity(HttpServletRequest request,
			@RequestBody(required = false) String payload) {
		return EntryControllerFunctions.createEntry(historyService, request, payload,
				AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, AppConstants.HISTORY_URL, logger);
	}

	@GetMapping
	public ResponseEntity<String> retrieveTemporalEntity(HttpServletRequest request,
			@RequestParam(value = "limit", required = false) Integer limit,
			@RequestParam(value = "offset", required = false) Integer offset,
			@RequestParam(value = "qtoken", required = false) String qToken,
			@RequestParam(name = "options", required = false) List<String> options,
			@RequestParam(value = "count", required = false, defaultValue = "false") Boolean countResult) {
		return QueryControllerFunctions.queryForEntries(historyService, request, true, defaultLimit, maxLimit, true);
	}

	@GetMapping("/{entityId}")
	public ResponseEntity<String> retrieveTemporalEntityById(HttpServletRequest request,
			@PathVariable("entityId") String entityId) {
		return QueryControllerFunctions.getEntity(historyService, request, null, null, entityId, true, defaultLimit,
				maxLimit);

	}

	@DeleteMapping("/{entityId}")
	public ResponseEntity<String> deleteTemporalEntityById(HttpServletRequest request,
			@PathVariable("entityId") String entityId) {
		return EntryControllerFunctions.deleteEntry(historyService, request, entityId, logger);

	}

	@PostMapping("/{entityId}/attrs")
	public ResponseEntity<String> addAttrib2TemopralEntity(HttpServletRequest request,
			@PathVariable("entityId") String entityId, @RequestBody(required = false) String payload,
			@RequestParam(required = false, name = "options") String options) {
		return EntryControllerFunctions.appendToEntry(historyService, request, entityId, payload, options,
				AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, logger);
	}

	@DeleteMapping("/{entityId}/attrs/{attrId}")
	public ResponseEntity<String> deleteAttrib2TemporalEntity(HttpServletRequest request,
			@PathVariable("entityId") String entityId, @PathVariable("attrId") String attrId) {
		try {
			HttpUtils.validateUri(entityId);
			Context context = JsonLdProcessor.getCoreContextClone();
			List<Object> links = HttpUtils.getAtContext(request);
			context = context.parse(links, true);
			logger.trace("deleteAttrib2TemporalEntity :: started");
			logger.debug("entityId : " + entityId + " attrId : " + attrId);
			historyService.delete(HttpUtils.getHeaders(request), entityId, attrId, null, context,
					new BatchInfo(-1, -1));
			logger.trace("deleteAttrib2TemporalEntity :: completed");
			return ResponseEntity.noContent().build();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@PatchMapping("/{entityId}/attrs/{attrId}/{instanceId}")
	public ResponseEntity<String> modifyAttribInstanceTemporalEntity(HttpServletRequest request,
			@PathVariable("entityId") String entityId, @PathVariable("attrId") String attrId,
			@PathVariable("instanceId") String instanceId, @RequestBody(required = false) String payload) {
		try {
			HttpUtils.validateUri(entityId);
			HttpUtils.validateUri(instanceId);
			logger.trace("modifyAttribInstanceTemporalEntity :: started");
			logger.debug("entityId : " + entityId + " attrId : " + attrId + " instanceId : " + instanceId);
			Context context = JsonLdProcessor.getCoreContextClone();

			List<Object> linkHeaders = HttpUtils.getAtContext(request);
			boolean atContextAllowed = HttpUtils.doPreflightCheck(request, linkHeaders);
			context = context.parse(linkHeaders, true);

			@SuppressWarnings("unchecked")
			Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor.expand(linkHeaders,
					JsonUtils.fromString(payload), opts, AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, atContextAllowed)
					.get(0);
			// TODO : TBD- conflict between specs and implementation <mentioned no request
			// body in specs>
			historyService.modifyAttribInstanceTemporalEntity(HttpUtils.getHeaders(request), entityId, resolved, attrId,
					instanceId, context);
			logger.trace("modifyAttribInstanceTemporalEntity :: completed");
			return ResponseEntity.noContent().build();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@DeleteMapping("/{entityId}/attrs/{attrId}/{instanceId}")
	public ResponseEntity<String> deleteAtrribInstanceTemporalEntity(HttpServletRequest request,
			@PathVariable("entityId") String entityId, @PathVariable("attrId") String attrId,
			@PathVariable("instanceId") String instanceId) {
		try {
			logger.trace("deleteAtrribInstanceTemporalEntity :: started");
			HttpUtils.validateUri(entityId);
			HttpUtils.validateUri(instanceId);
			logger.debug("entityId : " + entityId + " attrId : " + attrId + " instanceId : " + instanceId);
			Context context = JsonLdProcessor.getCoreContextClone();
			List<Object> links = HttpUtils.getAtContext(request);
			context = context.parse(links, true);

			historyService.delete(HttpUtils.getHeaders(request), entityId, attrId, instanceId, context,
					new BatchInfo(-1, -1));
			logger.trace("deleteAtrribInstanceTemporalEntity :: completed");
			return ResponseEntity.noContent().build();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}
}
