package eu.neclab.ngsildbroker.atcontextserver.controller;

import java.util.List;

import javax.inject.Singleton;
import org.jboss.resteasy.reactive.RestResponse;
import io.vertx.core.http.HttpServerRequest;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
@Path("ngsi-ld/contextes")
public class AtContextServerController {
	private final static Logger logger = LogManager.getLogger(AtContextServerController.class);

	/*
	 * @Autowired AtContext atContext;
	 */

	/*
	 * @Autowired ResourceLoader resourceLoader;
	 */

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
	@GET
	Path("/{contextId}")
	public RestResponse<Object> getContextForEntity( HttpServerRequest request,
			@PathParam("contextId") String contextId) {
		logger.trace("getAtContext() for " + contextId);
		/*
		 * if(contextId.equals(AppConstants.CORE_CONTEXT_URL_SUFFIX)) { return
		 * ResponseEntity.accepted().contentType(MediaType.APPLICATION_JSON).body(
		 * coreContext); }
		 */
		List<Object> contextes = null;// atContext.getContextes(contextId);
		StringBuilder body = new StringBuilder("{\"@context\": ");

		// body.append(DataSerializer.toJson(contextes));
		body.append("}");
		return RestResponse.accepted().contentType(MediaType.APPLICATION_JSON).body(body.toString());
	}

	@GET
	public RestResponse<Object> getAllContextes() {
		StringBuilder body = new StringBuilder("{\n");
		// Manuallly done because gson shows the actual byte values and not a string
//		Map<String, byte[]> contextMapping = null;//atContext.getAllContextes();
//		for(Entry<String, byte[]> contextEntry: contextMapping.entrySet()) {
//			body.append("    \"" + contextEntry.getKey() + "\": \"" + new String(contextEntry.getValue()) + "\",\n");
//		}
		body.append("}");
		return RestResponse.accepted().contentType(MediaType.APPLICATION_JSON).body(body.toString());
	}

}
