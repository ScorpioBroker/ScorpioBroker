package eu.neclab.ngsildbroker.atcontextserver.controller;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
	
	String coreContext;
	
	@PostConstruct
	private void setup() {
		try {
			coreContext = Files.readString(resourceLoader.getResource("classpath:ngsi-ld-core-context.jsonld").getFile().toPath());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	 
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
		if(contextId.equals("ngsi-ld-core-context")) {
			return ResponseEntity.accepted().contentType(MediaType.APPLICATION_JSON).body(coreContext);
		}
		List<Object> contextes = atContext.getContextes(contextId);
		StringBuilder body = new StringBuilder("{\"@context\": ");

		body.append(DataSerializer.toJson(contextes));
		body.append("}");
		return ResponseEntity.accepted().contentType(MediaType.APPLICATION_JSON).body(body.toString());
	}

	@GetMapping(name="atcontextget")
	public ResponseEntity<Object> getAllContextes() {
		return ResponseEntity.accepted().body(atContext.getAllContextes().toString());
	}

}
