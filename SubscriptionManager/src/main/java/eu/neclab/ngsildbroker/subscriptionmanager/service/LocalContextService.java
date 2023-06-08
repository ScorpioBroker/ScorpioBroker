package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.util.Map;

import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.smallrye.mutiny.Uni;

@Path("/ngsi-ld/v1/jsonldContexts")
@RegisterRestClient(configKey = "atcontext-service")
@ClientHeaderParam(name = "Accept", value = "application/json")
public interface LocalContextService {

	@POST
	@Path("/createimplicitly")
	Uni<String> createImplicitly(@HeaderParam(NGSIConstants.TENANT_HEADER) String tenant,
			Map<String, Object> payload);

}
