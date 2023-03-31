package eu.neclab.ngsildbroker.registryhandler.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.apache.kafka.common.protocol.types.Field;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.registryhandler.service.CSourceService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.http.HttpServerRequest;

/**
 * 
 * @version 1.0
 * @date 20-Jul-2018
 */
@Singleton
@Path("/ngsi-ld/v1/csourceRegistrations")
public class RegistryController {
	private final static Logger logger = LoggerFactory.getLogger(RegistryController.class);

	@Inject
	CSourceService csourceService;
	@ConfigProperty(name = "scorpio.entity.default-limit", defaultValue = "50")
	int defaultLimit;
	@ConfigProperty(name = "scorpio.entity.max-limit", defaultValue = "1000")
	int maxLimit;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@GET
	public Uni<RestResponse<Object>> queryCSource(HttpServerRequest request, @QueryParam(value = "limit") Integer limit,
			@QueryParam(value = "offset") Integer offset, @QueryParam(value = "qtoken") String qToken,
			@QueryParam(value = "count") boolean count) {
		// return QueryControllerFunctions.queryForEntries(csourceService, request,
		// false, defaultLimit, maxLimit, false);
		return null;
	}

	@POST
	public Uni<RestResponse<Object>> registerCSource(HttpServerRequest request, String payload) {

		Tuple2<Context, Map<String, Object>> tuple;
		try {
			tuple = HttpUtils.expandBody(request, payload, AppConstants.CSOURCE_REG_CREATE_PAYLOAD);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return csourceService.createRegistration(HttpUtils.getTenant(request), tuple.getItem2()).onItem()
				.transform(opResult -> {
					return HttpUtils.generateCreateResult(opResult, AppConstants.CSOURCE_URL);
				}).onFailure().recoverWithItem(error -> {
					return HttpUtils.handleControllerExceptions(error);
				});

	}

	@Path("/{registrationId}")
	@GET
	public Uni<RestResponse<Object>> getCSourceById(HttpServerRequest request,
			@PathParam("registrationId") String registrationId) {
		logger.debug("get CSource() ::" + registrationId);
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}

		Context context;
		List<Object> headerContext;
		try {
			HttpUtils.validateUri(registrationId);
			headerContext = HttpUtils.getAtContext(request);
			context = JsonLdProcessor.getCoreContextClone().parse(headerContext, false);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return csourceService.retrieveRegistration(HttpUtils.getTenant(request), registrationId).onItem()
				.transform(entity -> {
					return HttpUtils.generateEntityResult(headerContext, context, acceptHeader, entity, null, null,null);
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	@Path("/{registrationId}")
	@PATCH
	public Uni<RestResponse<Object>> updateCSource(HttpServerRequest request,
			@PathParam("registrationId") String registrationId, String payload) {
		Tuple2<Context, Map<String, Object>> tuple;
		try {
			tuple = HttpUtils.expandBody(request, payload, AppConstants.CSOURCE_REG_UPDATE_PAYLOAD);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return csourceService.updateRegistration(HttpUtils.getTenant(request), registrationId, tuple.getItem2())
				.onItem().transform(opResult -> {
					return HttpUtils.generateUpdateResultResponse(opResult);
				}).onFailure().recoverWithItem(error -> {
					return HttpUtils.handleControllerExceptions(error);
				});
	}

	@Path("/{registrationId}")
	@DELETE
	public Uni<RestResponse<Object>> deleteCSource(HttpServerRequest request,
			@PathParam("registrationId") String registrationId) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		try {
			HttpUtils.validateUri(registrationId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return csourceService.deleteRegistration(HttpUtils.getTenant(request), registrationId).onItem()
				.transform(opResult -> {

					return HttpUtils.generateDeleteResult(opResult);
				}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e));
	}

}
