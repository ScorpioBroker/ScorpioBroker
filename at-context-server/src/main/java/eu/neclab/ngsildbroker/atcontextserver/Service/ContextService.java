package eu.neclab.ngsildbroker.atcontextserver.Service;

import eu.neclab.ngsildbroker.atcontextserver.ContextCache;
import eu.neclab.ngsildbroker.atcontextserver.Dao.ContextDao;
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
import java.util.HashMap;
import java.util.Map;

@ApplicationScoped
public class ContextService {
    @Inject
    ContextDao dao;

    @Inject
    ContextCache cache;

    WebClient webClient;

    @PostConstruct
    void init() {
        webClient = WebClient.create(Vertx.vertx());
    }

    public Uni<RestResponse<Object>> getContextById(String id, boolean details) {
        return dao.getById(id, details).onItem().transformToUni(res -> {
            if (res.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
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
        if (kind != null && kind.equalsIgnoreCase("cached")) {
            return Uni.createFrom().failure(new ResponseException(ErrorType.OperationNotSupported));
        } else
            return dao.getContexts(kind, details).onItem()
                    .transformToUni(list -> Uni.createFrom().item(RestResponse.ok(list)));
    }

    public Uni<RestResponse<Object>> createOrGet(String url, String type) {// create cache or implicitly created context
        if (type != null && type.equals("implicitlyCreated")) {
            return dao.getById(url, false)
                    .onItem().transformToUni(res -> {
                        if (res.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                            return webClient.getAbs(url)
                                    .send()
                                    .onItemOrFailure()
                                    .transformToUni(
                                            (body, failure) -> {
                                                if (failure != null)
                                                    return Uni.createFrom().item(RestResponse.notFound());
                                                Map<String, Object> contextBody = (Map<String, Object>) body
                                                        .bodyAsJsonObject().getMap().get("@context");
                                                if (contextBody != null) {
                                                    Map<String, Object> atContext = new HashMap<>();
                                                    atContext.put("@context", contextBody);
                                                    return dao.createContextImpl(atContext, url);
                                                }
                                                return null;
                                            });
                        } else
                            return Uni.createFrom().item(res);

                    });
        } else
            return cache.getCache(url, false, true);
    }
}
