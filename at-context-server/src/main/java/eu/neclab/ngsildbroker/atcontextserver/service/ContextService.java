package eu.neclab.ngsildbroker.atcontextserver.service;

import com.github.jsonldjava.core.JsonLdOptions;
import eu.neclab.ngsildbroker.atcontextserver.cache.ContextCache;
import eu.neclab.ngsildbroker.atcontextserver.dao.ContextDao;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import org.jboss.resteasy.reactive.RestResponse;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class ContextService {
    @Inject
    ContextDao dao;

    @Inject
    ContextCache cache;

    WebClient webClient;
    JsonLdOptions jsonLdOptions = new JsonLdOptions();
    
    @Inject
    Vertx vertx;

    @PostConstruct
    void init() {
        webClient = WebClient.create(vertx);
    }

    public Uni<RestResponse<Object>> getContextById(String id, boolean details) {
        return dao.getById(id, details).onItem().transformToUni(res -> {
            if (res.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                return cache.createOrGetCache(id, details, false).onItem().transformToUni(response -> {
                    if (response.getStatus() == RestResponse.notFound().getStatus()) {
                        return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
                    }
                    return Uni.createFrom().item(response);
                });
            }
            return Uni.createFrom().item(res);
        });
    }

    public Uni<RestResponse<Object>> createContextHosted(Map<String, Object> payload) {
        return dao.hostContext(payload);
    }

    public Uni<RestResponse<Object>> deleteById(String id, Boolean reload) {
        return dao.deleteById(id).onItem().transformToUni(
                response -> {
                    if (response.getStatus() == RestResponse.Status.NOT_FOUND.getStatusCode()) {
                        if (reload) {
                            return cache.reload(id);
                        } else
                            return cache.invalidate(id);
                    }
                    return Uni.createFrom().item(response);
                });
    }

    public Uni<RestResponse<Object>> getContexts(String kind, Boolean details) {
        if (kind != null && kind.equalsIgnoreCase(NGSIConstants.CACHED)) {
            return cache.getAllCache(details).map(RestResponse::ok);
        } else if (kind != null && !kind.equalsIgnoreCase(NGSIConstants.CACHED))
            return dao.getAllContexts(kind, details).onItem()
                    .transform(RestResponse::ok);
        else
            return cache.getAllCache(details).onItem()
                    .transformToUni(list1 -> dao.getAllContexts(kind, details).onItem()
                            .transform(list2 -> {
                                List<Object> finalList = new ArrayList<>();
                                finalList.addAll(list1);
                                finalList.addAll(list2);
                                return RestResponse.ok(finalList);
                            }));
    }

    public Uni<RestResponse<Object>> createOrGetCache(String url) {// create cache or implicitly created context
        return cache.createOrGetCache(url, false, true);
    }

    public Uni<RestResponse<Object>> createImplicitly(Map<String, Object> payload) {
        return dao.createContextImpl(payload);

    }
}
