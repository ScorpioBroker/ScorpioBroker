package eu.neclab.ngsildbroker.atcontextserver.Controller;

import eu.neclab.ngsildbroker.atcontextserver.Service.ContextService;
import io.smallrye.mutiny.Uni;
import org.jboss.resteasy.reactive.RestResponse;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;

@Path("/ngsi-ld/v1/jsonldContexts/")
public class ContextController {

    @Inject
    ContextService contextService;


    @GET
    @Path("{contextId}")
    public Uni<RestResponse<Object>> getContextById(@PathParam("contextId") String id, @QueryParam("details") boolean details) {
        return contextService.getContextById(id, details);
    }

    @GET
    public Uni<RestResponse<List<Object>>> getContexts(@QueryParam("kind") String kind,
                                                       @QueryParam("details") boolean details) {
        return contextService.getContexts(kind, details);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Uni<RestResponse<Object>> createContext(Map<String, Object> payload) {
        return contextService.createContext(payload);
    }

    @DELETE
    @Path("{contextId}")
    public Uni<RestResponse<Object>> deleteContextById(@PathParam("contextId") String id, @QueryParam("reload") boolean reload) {
        return contextService.deleteById(id, reload);
    }

}