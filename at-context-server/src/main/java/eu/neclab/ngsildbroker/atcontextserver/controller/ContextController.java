package eu.neclab.ngsildbroker.atcontextserver.controller;

import com.github.jsonldjava.utils.JsonUtils;
import eu.neclab.ngsildbroker.atcontextserver.service.ContextService;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import io.smallrye.mutiny.Uni;
import org.jboss.resteasy.reactive.RestResponse;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;

@Path("/ngsi-ld/v1/jsonldContexts/")
public class ContextController {

    @Inject
    ContextService contextService;

    @GET
    @Path("{contextId}")
    public Uni<RestResponse<Object>> getContextById(@PathParam("contextId") String id,
            @QueryParam("details") boolean details) {
        return contextService.getContextById(id, details)
                .onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
    }

    @GET
    public Uni<RestResponse<Object>> getContexts(@QueryParam("kind") String kind,
            @QueryParam("details") boolean details) {
        return contextService.getContexts(kind, details)
                .onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Uni<RestResponse<Object>> createContext(String payload) {
        Map<String, Object> payloadMap = new HashMap<>();
        try {
            Map<String, Object> contextBody = (Map<String, Object>) ((Map<String, Object>) JsonUtils
                    .fromString(payload)).get(NGSIConstants.JSON_LD_CONTEXT);
            if (contextBody == null)
                throw new Exception("Bad Request");
            else
                payloadMap.put(NGSIConstants.JSON_LD_CONTEXT, contextBody);
        } catch (Exception e) {
            return Uni.createFrom().item(
                    HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData)));
        }
        return contextService.createContextHosted(payloadMap)
                .onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
    }

    @DELETE
    @Path("{contextId}")
    public Uni<RestResponse<Object>> deleteContextById(@PathParam("contextId") String id,
            @QueryParam("reload") boolean reload) {
        return contextService.deleteById(id, reload)
                .onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
    }

    @GET
    @Path("/createcache/{url}")
    public Uni<RestResponse<Object>> loadCache(@PathParam("url") String url) {
        return contextService.createOrGetCache(url);
    }

    @POST
    @Path("/createimplicitly/")
    public Uni<RestResponse<Object>> createImplicitly(String payload) {
        Map<String, Object> payloadMap = new HashMap<>();
        try {
            Map<String, Object> contextBody = (Map<String, Object>) ((Map<String, Object>) JsonUtils
                    .fromString(payload)).get("@context");
            if (contextBody == null)
                throw new Exception("Bad Request");
            else
                payloadMap.put(NGSIConstants.JSON_LD_CONTEXT, contextBody);
        } catch (Exception e) {
            return Uni.createFrom().item(
                    HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData)));
        }
        return contextService.createImplicitly(payloadMap);
    }
}