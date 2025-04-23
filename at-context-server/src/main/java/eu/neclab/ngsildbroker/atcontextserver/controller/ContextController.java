package eu.neclab.ngsildbroker.atcontextserver.controller;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.atcontextserver.service.ContextService;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;

import org.apache.commons.lang3.StringUtils;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.RestResponse.ResponseBuilder;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Path("/ngsi-ld/v1/jsonldContexts/")
@SuppressWarnings("unchecked")
public class ContextController {

	@Inject
	ContextService contextService;

	private Set<String> allowedKinds = Sets.newHashSet("Cached", "Hosted", "ImplicitlyCreated");

	@GET
	@Path("{contextId}")
	public Uni<RestResponse<Object>> getContextById(@PathParam("contextId") String id,
			@QueryParam("details") String detailsS) {

		boolean details;
		try {
			details = HttpUtils.parseBoolean(detailsS);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, AppConstants.INTERNAL_NULL_KEY));
		}
		if (NGSIConstants.CORE_CONTEXT_URLS.contains(id)) {
			id = AppConstants.INTERNAL_NULL_KEY;
		}
		return contextService.getContextById(id, details).onItem().transform(
				context -> ResponseBuilder.ok().entity(context).header("Content-Type", "application/json").build())
				.onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, AppConstants.INTERNAL_NULL_KEY);
				});
	}

	@GET
	public Uni<RestResponse<Object>> getContexts(@QueryParam("kind") String kind,
			@QueryParam("details") String detailsS) {
		boolean details;
		try {
			details = HttpUtils.parseBoolean(detailsS);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, AppConstants.INTERNAL_NULL_KEY));
		}
		if (kind != null && !allowedKinds.contains(kind)) {
			return Uni.createFrom()
					.item(HttpUtils
							.handleControllerExceptions(
									new ResponseException(ErrorType.BadRequestData,
											"Allowed values for parameter kind are: "
													+ StringUtils.join(allowedKinds, ",") + ". You provided " + kind),
									AppConstants.INTERNAL_NULL_KEY));
		}
		return contextService.getContexts(kind, details).onItem().transform(
				context -> ResponseBuilder.ok().entity(context).header("Content-Type", "application/json").build())
				.onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, AppConstants.INTERNAL_NULL_KEY);
				});
	}

	@POST
	public Uni<RestResponse<Object>> createContext(String body) {

		Map<String, Object> context;
		try {
			context = new JsonObject(body).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, AppConstants.INTERNAL_NULL_KEY));
		}
		if (!context.containsKey(NGSIConstants.JSON_LD_CONTEXT)) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.InvalidRequest, "A Map with an @Context entry is required."),
					AppConstants.INTERNAL_NULL_KEY));
		}
		return contextService.createContextHosted(context).onItem()
				.transform(r -> HttpUtils.generateCreateResult(r, AppConstants.CONTEXTS_URL)).onFailure()
				.recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, AppConstants.INTERNAL_NULL_KEY);
				});

	}

	@DELETE
	@Path("{contextId}")
	public Uni<RestResponse<Object>> deleteContextById(@PathParam("contextId") String id,
			@QueryParam("reload") String reloadS) {
		boolean reload;
		try {
			reload = HttpUtils.parseBoolean(reloadS);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, AppConstants.INTERNAL_NULL_KEY));
		}
		if (NGSIConstants.CORE_CONTEXT_URLS.contains(id)) {
			id = AppConstants.INTERNAL_NULL_KEY;
		}
		if (id.equals(AppConstants.INTERNAL_NULL_KEY)) {
			if (reload) {
				return Uni.createFrom().item(RestResponse.noContent());
			}

			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(
							new ResponseException(ErrorType.BadRequestData, "You cannot delete scorpios core context"),
							AppConstants.INTERNAL_NULL_KEY));
		}
		return contextService.deleteById(id, reload).onItem().transform(v -> RestResponse.noContent()).onFailure()
				.recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, AppConstants.INTERNAL_NULL_KEY);
				});
	}

	@GET
	@Path("/createcache/{url}")
	public Uni<RestResponse<Object>> loadCache(@PathParam("url") String url) {
		if (NGSIConstants.CORE_CONTEXT_URLS.contains(url)) {
			url = AppConstants.INTERNAL_NULL_KEY;
		}
		return contextService.createOrGetCache(url).onItem().transform(
				context -> ResponseBuilder.ok().entity(context).header("Content-Type", "application/json").build());
	}

	@POST
	@Path("/createimplicitly/")
	public Uni<RestResponse<Object>> createImplicitly(String payload) {
		return JsonUtils.fromString(payload).onItem().transformToUni(json -> {
			Map<String, Object> payloadMap = new HashMap<>();
			try {
				Map<String, Object> contextBody = (Map<String, Object>) ((Map<String, Object>) json).get("@context");
				if (contextBody == null)
					throw new Exception("Bad Request");
				else
					payloadMap.put(NGSIConstants.JSON_LD_CONTEXT, contextBody);
			} catch (Exception e) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, AppConstants.INTERNAL_NULL_KEY));
			}
			return contextService.createImplicitly(payloadMap);
		});
	}
}