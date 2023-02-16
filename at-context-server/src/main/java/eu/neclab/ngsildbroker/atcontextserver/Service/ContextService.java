package eu.neclab.ngsildbroker.atcontextserver.Service;

import eu.neclab.ngsildbroker.atcontextserver.Dao.ContextDao;
import io.smallrye.mutiny.Uni;
import org.jboss.resteasy.reactive.RestResponse;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@ApplicationScoped
public class ContextService {
    @Inject
    ContextDao dao;

    public Uni<RestResponse<Object>> getContextById(String id, boolean details) {
        return dao.getById(id, details).onItem().transformToUni(
                res -> {
                    if (res.getStatus() != RestResponse.Status.OK.getStatusCode() && id.toLowerCase().startsWith("http")) {
                        return Uni.createFrom().item(res); //ToDo check in cache
                    } else return Uni.createFrom().item(res);
                });
    }

    public Uni<RestResponse<Object>> createContext(Map<String, Object> payload) {
        Map<String, Object> context = (Map<String, Object>) payload.get("@context");
        if (context == null) return Uni.createFrom().item(RestResponse.status(RestResponse.Status.BAD_REQUEST));
        Map<String, Object> body = new HashMap<>();
        body.put("@context", context);
        return dao.addContext(body);
    }

    public Uni<RestResponse<Object>> deleteById(String id, Boolean reload) {
        return dao.deleteById(id).onItem().transformToUni(
                response -> {
                    if (reload && response.getStatus() == RestResponse.Status.NOT_FOUND.getStatusCode()
                            && id.toLowerCase().startsWith("http")
                    ) {
                        return Uni.createFrom().item(response); //ToDo check in cache
                    }
                    return Uni.createFrom().item(response);
                }
        );
    }

    public Uni<RestResponse<List<Object>>> getContexts(String kind, Boolean details) {
        return dao.getContexts(kind, details).onItem()
                .transformToUni(list -> {
                    if (kind == null || kind.equals("")) {
                        return Uni.createFrom().item(RestResponse.ok(list)); //ToDo check in cache
                    } else if (kind.equalsIgnoreCase("cached")) {
                        return Uni.createFrom().item(RestResponse.ok(list)); //ToDo check in cache
                    } else return Uni.createFrom().item(RestResponse.ok(list));

                });
    }

}
