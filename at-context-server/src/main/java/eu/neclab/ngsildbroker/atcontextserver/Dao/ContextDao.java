package eu.neclab.ngsildbroker.atcontextserver.Dao;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.Tuple;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.jaxrs.RestResponseBuilderImpl;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@ApplicationScoped
public class ContextDao {
    @Inject
    PgPool pgPool;

    @PostConstruct
    void setup() {

    }

    public Uni<RestResponse<Object>> getById(String id, Boolean details) {
        String sql = "SELECT %s FROM public.contexts WHERE id=$1";
        if (details)
            sql = sql.formatted("*");
        else
            sql = sql.formatted("body");
        return pgPool.preparedQuery(sql).execute(Tuple.of(id)).onItem()
                .transformToUni(rows -> {
                    if (rows.size() > 0) {
                        Row row = rows.iterator().next();
                        Map<String, Object> map = row.toJson().getMap();
                        if (details) {
                            map.put(row.getColumnName(1), ((JsonObject) map.get(row.getColumnName(1))).getMap());
                            map.put("url", "http://localhost:9090/ngsi-ld/v1/jsonldContexts/" + map.get("id"));
                            return Uni.createFrom().item(RestResponse.ok(map));
                        } else
                            return Uni.createFrom().item(RestResponse.ok(row.getJson("body")));

                    } else
                        return Uni.createFrom().item(RestResponse.notFound());
                });
    }

    public Uni<RestResponse<Object>> hostContext(Map<String, Object> payload) {
        String sql = "INSERT INTO public.contexts (id, body, kind) values($1, $2, 'hosted') returning id";
        return pgPool.preparedQuery(sql).execute(Tuple.of("urn:" + UUID.randomUUID(), new JsonObject(payload))).onItem()
                .transformToUni(rows -> {
                    if (rows.size() > 0) {
                        return Uni.createFrom().item( RestResponseBuilderImpl.create(201)
                                .entity(new JsonObject("{\"url\":\"" +"http://localhost:9090/ngsi-ld/v1/jsonldContexts/" +rows.iterator().next().getString(0) + "\"}"))
                                .build());
                    } else
                        return Uni.createFrom().failure(new Throwable("Server Error"));
                });

    }

    public Uni<RestResponse<Object>> deleteById(String id) {
        String sql = "DELETE FROM public.contexts WHERE id=$1 RETURNING id";
        return pgPool.preparedQuery(sql).execute(Tuple.of(id)).onItem()
                .transformToUni(rows -> {
                    if (rows.size() > 0)
                        return Uni.createFrom().item(RestResponseBuilderImpl.noContent().build());
                    else
                        return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
                });
    }

    public Uni<List<Object>> getContexts(String kind, Boolean details) {
        String sql;
        if (details)
            sql = "Select * from public.contexts ";
        else
            sql = "Select id from public.contexts ";
        if (kind != null)
            sql += "where kind='%s'".formatted(kind.toLowerCase());
        List<Object> contexts = new ArrayList<>();
        return pgPool.preparedQuery(sql).execute()
                .onItem()
                .transform(rows -> {
                    rows.forEach(i -> {
                        Map<String, Object> map = i.toJson().getMap();

                        if (details) {
                            map.put(i.getColumnName(1), ((JsonObject) map.get(i.getColumnName(1))).getMap());
                            map.put("url", "http://localhost:9090/ngsi-ld/v1/jsonldContexts/" + URLEncoder.encode( map.get("id").toString(), StandardCharsets.UTF_8));
                            contexts.add(map);
                        } else {
                            contexts.add("http://localhost:9090/ngsi-ld/v1/jsonldContexts/" + URLEncoder.encode(map.get("id").toString(), StandardCharsets.UTF_8));
                        }

                    });
                    return contexts;
                });

    }

    public Uni<RestResponse<Object>> createContextImpl(Map<String, Object> payload, String id) {
        String sql = "INSERT INTO public.contexts (id, body, kind) values($1, $2, 'implicitlyCreated') returning body";
        return pgPool.preparedQuery(sql).execute(Tuple.of(id, new JsonObject(payload))).onItem()
                .transform(rows -> {
                    if (rows.size() > 0) {
                        return RestResponse.ok(rows.iterator().next().getJsonObject(0).getMap().get("body"));
                    } else
                        return RestResponseBuilderImpl.create(500).build();
                });
    }
}
