package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.quarkus.rest.client.reactive.ClientQueryParam;
import io.smallrye.mutiny.Uni;

@Path("/ngsi-ld/v1/entities")
@RegisterRestClient(configKey = "entity-service")
@ClientHeaderParam(name = "Accept", value = "application/json")
public interface LocalEntityService {

	@GET
	@Path("/{entityId}")
	@ClientQueryParam(name = NGSIConstants.QUERY_PARAMETER_OPTIONS, value = NGSIConstants.QUERY_PARAMETER_OPTIONS_SYSATTRS)
	Uni<Map<String, Object>> getEntityById(@HeaderParam(NGSIConstants.TENANT_HEADER) String tenant,
			@PathParam("entityId") String entityId);

}
