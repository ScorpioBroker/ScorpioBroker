package eu.neclab.ngsildbroker.atcontextserver.controller;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.io.Files;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.ldcontext.AtContext;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;

@RestController
@RequestMapping("ngsi-ld/contextes")
public class AtContextServerController {
	private final static Logger logger = LogManager.getLogger(AtContextServerController.class);

	@Autowired
	AtContext atContext;

	
	@Autowired
	ResourceLoader resourceLoader;
	
	/*
	 * String coreContext;
	 * 
	 * @PostConstruct private void setup() { try { coreContext = new
	 * String(Files.asByteSource(resourceLoader.getResource(
	 * "classpath:ngsi-ld-core-context.jsonld").getFile()).read()); } catch
	 * (IOException e) { // TODO Auto-generated catch block e.printStackTrace(); }
	 * 
	 * }
	 */
	 
	/**
	 * Method(GET) for multiple attributes separated by comma list
	 * 
	 * @param request
	 * @param entityId
	 * @param attrs
	 * @return
	 */
	@GetMapping(path = "/{contextId}")
	public ResponseEntity<Object> getContextForEntity(HttpServletRequest request,
			@PathVariable("contextId") String contextId) {
		logger.trace("getAtContext() for " + contextId);
		/*
		 * if(contextId.equals(AppConstants.CORE_CONTEXT_URL_SUFFIX)) { return
		 * ResponseEntity.accepted().contentType(MediaType.APPLICATION_JSON).body(
		 * coreContext); }
		 */
		List<Object> contextes = atContext.getContextes(contextId);
		StringBuilder body = new StringBuilder("{\"@context\": ");

		body.append(DataSerializer.toJson(contextes));
		body.append("}");
		return ResponseEntity.accepted().contentType(MediaType.APPLICATION_JSON).body(body.toString());
	}

	@GetMapping(name="atcontextget")
	public ResponseEntity<Object> getAllContextes() {
		StringBuilder body = new StringBuilder("{\n");
		//Manuallly done because gson shows the actual byte values and not a string
		Map<String, byte[]> contextMapping = atContext.getAllContextes();
		for(Entry<String, byte[]> contextEntry: contextMapping.entrySet()) {
			body.append("    \"" + contextEntry.getKey() + "\": \"" + new String(contextEntry.getValue()) + "\",\n");
		}
		body.append("}");
		return ResponseEntity.accepted().contentType(MediaType.APPLICATION_JSON).body(body.toString());
	}

}
