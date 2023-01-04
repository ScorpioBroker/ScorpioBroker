package eu.neclab.ngsildbroker.queryhandler.controller;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.net.HttpHeaders;
import eu.neclab.ngsildbroker.commons.datatypes.terms.*;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.PayloadQueryParamParser;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.queryhandler.services.EntityPostQueryParser;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static eu.neclab.ngsildbroker.commons.tools.HttpUtils.INVALID_HEADER;

@Singleton
@Path("/ngsi-ld/v1/entityOperations")
public class EntityOperationQueryController {

    @Inject
    QueryService queryService;

    @ConfigProperty(name = "scorpio.entity.default-limit", defaultValue = "50")
    int defaultLimit;

    @ConfigProperty(name = "scorpio.entity.batch-operations.query.max-limit", defaultValue = "1000")
    int maxLimit;

    @ConfigProperty(name = "scorpio.entity.batch-operations.query.max-limit", defaultValue = "1000")
    String coreContext;

    private PayloadQueryParamParser paramParser = new EntityPostQueryParser();

    @PostConstruct
    public void init() {
        JsonLdProcessor.init(coreContext);
    }

    @Path("/query")
    @POST
    public Uni<RestResponse<Object>> postQuery(HttpServerRequest request, String payload,
                                               @QueryParam(value = "limit") AtomicInteger limit,
                                               @QueryParam(value = "offset") Integer offset,
                                               @QueryParam(value = "qtoken") String qToken,
                                               @QueryParam(value = "options") List<String> options,
                                               @QueryParam(value = "count") Boolean count,
                                               @QueryParam(value = "lang") String lang,
                                               @QueryParam(value = "localOnly") Boolean localOnly
    ) {
        return HttpUtils.getAtContext(request).onItem().transformToUni(headerContext -> {
            ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
            int acceptHeader = HttpUtils.parseAcceptHeader(headers.get(HttpHeaders.ACCEPT));
            if (acceptHeader == -1) {
                return INVALID_HEADER;
            }
            if (limit.get() == 0) {
                limit.set(defaultLimit);
            }
            if (limit.get() > maxLimit) {
                return Uni.createFrom()
                        .item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.TooManyResults)));
            }
            Map<String, Object> originalPayload;
            try {
                originalPayload = (Map<String, Object>) JsonUtils.fromString(payload);
            } catch (IOException e) {
                return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
            }
            List<Map<String, Object>> entities = (List<Map<String, Object>>) originalPayload.get("entities");
            Context context = JsonLdProcessor.getCoreContextClone().parse(headerContext, true);
            return queryService.postQuery(HttpUtils.getTenant(request), entities, lang, limit.get(), offset, count, localOnly,
                    context).onItem().transform(RestResponse::ok);

        });
    }
}
