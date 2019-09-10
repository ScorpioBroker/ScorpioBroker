package eu.neclab.ngsildbroker.atcontextserver.controller;

import java.util.List;
import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import eu.neclab.ngsildbroker.commons.ldcontext.AtContext;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;

@RestController
public class AtContextServerController {
	private final static Logger logger = LogManager.getLogger(AtContextServerController.class);

	@Autowired
	AtContext atContext;

	/**
	 * Method(GET) for multiple attributes separated by comma list
	 * 
	 * @param request
	 * @param entityId
	 * @param attrs
	 * @return
	 */
	@GetMapping(path = "/{entityId}")
	public ResponseEntity<Object> getContextForEntity(HttpServletRequest request,
			@PathVariable("entityId") String entityId) {
		logger.trace("getAtContext() for " + entityId);
		List<Object> contextes = atContext.getContextes(entityId);
		StringBuilder body = new StringBuilder("{\"@context\": ");

		body.append(DataSerializer.toJson(contextes));
		body.append("}");
		return ResponseEntity.accepted().contentType(MediaType.APPLICATION_JSON).body(body.toString());
	}

	@GetMapping
	public ResponseEntity<Object> getAllContextes() {
		return ResponseEntity.accepted().body(atContext.getAllContextes().toString());
	}

}
