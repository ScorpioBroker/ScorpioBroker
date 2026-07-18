package eu.neclab.ngsildbroker.atcontextserver.controller;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.atcontextserver.service.ContextService;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.ParsedQueryParams;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.NgsiLdOperation;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.QueryParamParser;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.metrics.MetricUnits;
import org.eclipse.microprofile.metrics.annotation.ConcurrentGauge;
import org.eclipse.microprofile.metrics.annotation.Counted;
import org.eclipse.microprofile.metrics.annotation.Timed;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.RestResponse.ResponseBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Path("/ngsi-ld/v1/jsonldContexts/")
@SuppressWarnings("unchecked")
public class ContextController {

	private final static Logger logger = LoggerFactory.getLogger(ContextController.class);
	@Inject
	ContextService contextService;

	private Set<String> allowedKinds = Sets.newHashSet("Cached", "Hosted", "ImplicitlyCreated");

	@GET
	@Path("{contextId}")
	@Counted(name = "context_retrieve_total", description = "Total number of context retrieve requests", absolute = true)
	@Timed(name = "context_retrieve_duration", description = "Duration of context retrieve requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "context_retrieve_concurrent", description = "Number of concurrent context retrieve requests", absolute = true)
	public Uni<RestResponse<Object>> getContextById(HttpServerRequest request, @PathParam("contextId") String id) {

		boolean details;
		try {
			ParsedQueryParams queryParams = QueryParamParser.parse(request, NgsiLdOperation.RETRIEVE_CONTEXT);
			details = queryParams.getBoolean(NGSIConstants.QUERY_PARAMETER_DETAILS);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, AppConstants.INTERNAL_NULL_KEY));
		}
		if (NGSIConstants.CORE_CONTEXT_URLS.contains(id)) {
			id = AppConstants.INTERNAL_NULL_KEY;
		}
		return contextService.getContextById(id, details).onItem().transform(
				context -> {
					logger.debug("sending context response");

					return ResponseBuilder.ok().entity(context).header("Content-Type", "application/json").build();
				})
				.onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, AppConstants.INTERNAL_NULL_KEY);
				});
	}

	@GET
	public Uni<RestResponse<Object>> getContexts(HttpServerRequest request) {
		String kind;
		boolean details;
		try {
			ParsedQueryParams queryParams = QueryParamParser.parse(request, NgsiLdOperation.QUERY_CONTEXTS);
			kind = queryParams.getString(NGSIConstants.QUERY_PARAMETER_KIND);
			details = queryParams.getBoolean(NGSIConstants.QUERY_PARAMETER_DETAILS);
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
	@Counted(name = "context_create_total", description = "Total number of context create requests", absolute = true)
	@Timed(name = "context_create_duration", description = "Duration of context create requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "context_create_concurrent", description = "Number of concurrent context create requests", absolute = true)
	public Uni<RestResponse<Object>> createContext(HttpServerRequest request, String body) {

		Map<String, Object> context;
		try {
			QueryParamParser.parse(request, NgsiLdOperation.CREATE_CONTEXT);
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
	@Counted(name = "context_delete_total", description = "Total number of context delete requests", absolute = true)
	@Timed(name = "context_delete_duration", description = "Duration of context delete requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "context_delete_concurrent", description = "Number of concurrent context delete requests", absolute = true)
	public Uni<RestResponse<Object>> deleteContextById(HttpServerRequest request,
			@PathParam("contextId") String id) {
		boolean reload;
		try {
			ParsedQueryParams queryParams = QueryParamParser.parse(request, NgsiLdOperation.DELETE_CONTEXT);
			reload = queryParams.getBoolean(NGSIConstants.QUERY_PARAMETER_RELOAD);
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
	@Counted(name = "context_cache_total", description = "Total number of context cache requests", absolute = true)
	@Timed(name = "context_cache_duration", description = "Duration of context cache requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "context_cache_concurrent", description = "Number of concurrent context cache requests", absolute = true)
	public Uni<RestResponse<Object>> loadCache(HttpServerRequest request, @PathParam("url") String url) {
		try {
			QueryParamParser.parse(request, NgsiLdOperation.CACHE_CONTEXT);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, AppConstants.INTERNAL_NULL_KEY));
		}
		if (NGSIConstants.CORE_CONTEXT_URLS.contains(url)) {
			url = AppConstants.INTERNAL_NULL_KEY;
		}
		return contextService.createOrGetCache(url).onItem().transform(
				context -> ResponseBuilder.ok().entity(context).header("Content-Type", "application/json").build());
	}

	@POST
	@Path("/createimplicitly/")
	@Counted(name = "context_createimplicitly_total", description = "Total number of context createimplicitly requests", absolute = true)
	@Timed(name = "context_createimplicitly_duration", description = "Duration of context createimplicitly requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "context_createimplicitly_concurrent", description = "Number of concurrent context createimplicitly requests", absolute = true)
	public Uni<RestResponse<Object>> createImplicitly(HttpServerRequest request, String payload) {
		String tenant = HttpUtils.getTenant(request);
		try {
			QueryParamParser.parse(request, NgsiLdOperation.CREATE_CONTEXT_IMPLICIT);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		return JsonUtils.fromString(payload).onItem().transformToUni(json -> {
			Map<String, Object> payloadMap = new HashMap<>();
			try {
				Map<String, Object> contextBody = (Map<String, Object>) ((Map<String, Object>) json).get("@context");
				if (contextBody == null)
					throw new Exception("Bad Request");
				else
					payloadMap.put(NGSIConstants.JSON_LD_CONTEXT, contextBody);
			} catch (Exception e) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
			}
			return contextService.createImplicitly(tenant, payloadMap);
		});
	}
}