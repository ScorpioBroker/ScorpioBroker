package eu.neclab.ngsildbroker.subscriptionmanager.service;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.quarkus.rest.client.reactive.ClientQueryParam;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import java.util.List;
import java.util.Map;

@Path("/ngsi-ld/v1/entities")
@RegisterRestClient(configKey = "entity-service")
@ClientHeaderParam(name = "Accept", value = "application/json")
public interface LocalEntityService {

	@GET
	@Path("/{entityId}")
	@ClientQueryParam(name = NGSIConstants.QUERY_PARAMETER_OPTIONS, value = NGSIConstants.QUERY_PARAMETER_OPTIONS_SYSATTRS)
	Uni<Map<String, Object>> getEntityById(@HeaderParam(NGSIConstants.TENANT_HEADER) String tenant,
			@PathParam("entityId") String entityId, @QueryParam("doNotCompact") boolean doNotCompact);

	@GET
	@Path("/")
	Uni<List<Map<String, Object>>> getAllByIds(@HeaderParam(NGSIConstants.TENANT_HEADER) String tenant,
			@QueryParam("id") String ids, @QueryParam("doNotCompact") boolean doNotCompact);

	@GET
	@Path("/")
	Uni<List<Map<String, Object>>> query(@HeaderParam(NGSIConstants.TENANT_HEADER) String tenant,
			@QueryParam("id") String id, @QueryParam("type") String typeQuery,
			@QueryParam("idPattern") String idPattern, @QueryParam("attrs") String attrs, @QueryParam("q") String q,
			@QueryParam("csf") String csf, @QueryParam("geometry") String geometry, @QueryParam("georel") String georel,
			@QueryParam("coordinates") String coordinates, @QueryParam("geoproperty") String geoproperty,
			@QueryParam("geometryProperty") String geometryProperty, @QueryParam("lang") String lang,
			@QueryParam("scopeQ") String scopeQ, @QueryParam("localOnly") boolean localOnly,
			@QueryParam("options") String options, @QueryParam("limit") Integer limit,
			@QueryParam("doNotCompact") boolean doNotCompact);

}
