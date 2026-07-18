package eu.neclab.ngsildbroker.infomanager.controller;

import com.google.common.collect.Maps;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.NgsiLdOperation;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.commons.tools.QueryParamParser;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import org.jboss.resteasy.reactive.RestResponse;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Map;

@Path("/ngsi-ld/v1/info")
public class InfoController {

	@Inject
	MicroServiceUtils microServiceUtils;

	@GET
	@Path("/sourceIdentity")
	public Uni<RestResponse<Object>> getSourceIdentity(HttpServerRequest request) {
		String tenant = HttpUtils.getTenant(request);
		try {
			QueryParamParser.parse(request, NgsiLdOperation.INFO);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		String sourceAlias = microServiceUtils.getGatewayString() + tenant;
		Map<String, Object> sourceIdentity = Maps.newHashMap();
		sourceIdentity.put(NGSIConstants.ID, sourceAlias);
		sourceIdentity.put(NGSIConstants.TYPE, NGSIConstants.CONTEXT_SOURCE_IDENTITY_SHORT);
		sourceIdentity.put(NGSIConstants.NGSI_LD_SOURCE_ALIAS, sourceAlias);
		long uptime = ManagementFactory.getRuntimeMXBean().getUptime();
		sourceIdentity.put(NGSIConstants.NGSI_LD_SOURCE_UPTIME_SHORT, Duration.ofMillis(uptime).toString());
		sourceIdentity.put(NGSIConstants.NGSI_LD_SOURCE_TIME_AT_SHORT,
				SerializationTools.toDateTimeString(System.currentTimeMillis()));

		return Uni.createFrom().item(RestResponse.ok(sourceAlias));

	}

}