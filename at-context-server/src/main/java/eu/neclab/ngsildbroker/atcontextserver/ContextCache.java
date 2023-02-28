package eu.neclab.ngsildbroker.atcontextserver;

import io.quarkus.cache.*;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import org.jboss.resteasy.reactive.RestResponse;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.sql.Timestamp;
import java.util.*;

@ApplicationScoped
public class ContextCache {
    @Inject
    Vertx vertx;
    WebClient webClient;
    @CacheName("context")
    Cache cache;

    @PostConstruct
    void init() {
        webClient = WebClient.create(vertx);
    }

    @CacheResult(cacheName = "context")
    public Uni<Map<String, Object>> load(String uri) {
        return webClient.getAbs(uri)
                .send()
                .onItemOrFailure().transform(
                        (res, failure) -> {
                            Map<String, Object> map = new HashMap<>();
                            if (failure != null)
                                return map;
                            Map<String, Object> context = new HashMap<>();
                            Object contextToPut = res.getDelegate().bodyAsJsonObject().getMap().get("@context");
                            if (contextToPut == null)
                                return null;
                            context.put("@context", contextToPut);
                            map.put("body", context);
                            map.put("kind", "cached");
                            map.put("createdat", new Timestamp(System.currentTimeMillis()));
                            map.put("url", uri);
                            map.put("id", uri);
                            return map;
                        });
    }

    public Uni<RestResponse<Object>> getCache(String uri, Boolean details, Boolean loadNewCache) {
        Set<Object> cacheSet = cache.as(CaffeineCache.class).keySet();
        if (!loadNewCache && !cacheSet.contains(uri))
            return Uni.createFrom().item(RestResponse.notFound());
        if (details)
            return load(uri).onItem().transform(RestResponse::ok);
        else
            return load(uri).onItem()
                    .transform(map -> RestResponse.ok(map.get("body")));
    }

    public Uni<List<Object>> getAllCache(Boolean details) {
        Set<Object> set = cache.as(CaffeineCache.class).keySet();
        List<Object> list = new ArrayList<>();
        set.forEach(key -> {
            try {
                Map<String, Object> cachedItem = (Map<String, Object>) cache.as(CaffeineCache.class).getIfPresent(key)
                        .get();
                if (details)
                    list.add(cachedItem);
                else
                    list.add(cachedItem.get("url"));
            } catch (Exception ignored) {

            }
        });
        return Uni.createFrom().item(list);
    }

    @CacheInvalidate(cacheName = "context")
    public Uni<RestResponse<Object>> reload(@CacheKey String uri) {
        return load(uri).onItem().transform(res -> RestResponse.ok());
    }

    @CacheInvalidate(cacheName = "context")
    public Uni<RestResponse<Object>> invalidate(@CacheKey String uri) {
        return Uni.createFrom().item(RestResponse.noContent());
    }

    @CacheInvalidateAll(cacheName = "context")
    @Scheduled(every = "20m")
    public void invalidateAll() {
        System.out.println("Cache Invalidated!");
    }
}
