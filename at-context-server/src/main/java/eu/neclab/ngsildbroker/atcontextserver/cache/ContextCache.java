package eu.neclab.ngsildbroker.atcontextserver.cache;

import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.RemoteDocument;
import io.quarkus.cache.*;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import org.jboss.resteasy.reactive.RestResponse;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import java.sql.Timestamp;
import java.util.*;

@ApplicationScoped
public class ContextCache {
    @CacheName("context")
    Cache cache;
    JsonLdOptions jsonLdOptions = new JsonLdOptions();
    @PostConstruct
    void init() {

    }

    @CacheResult(cacheName = "context")
    public Uni<Map<String, Object>> load(String uri) {
        try {
            RemoteDocument rd = jsonLdOptions.getDocumentLoader().loadDocument(uri);
            if (rd.getDocument() instanceof Map<?, ?> map && map.containsKey("@context")) {
                Map<String, Object> contextBody = (Map<String, Object>) map.get("@context");
                Map<String, Object> finalContext = new HashMap<>();
                Map<String, Object> tempMap = new HashMap<>();
                tempMap.put("@context", contextBody);
                finalContext.put("body", tempMap);
                finalContext.put("kind", "cached");
                finalContext.put("createdat", new Timestamp(System.currentTimeMillis()));
                finalContext.put("url", uri);
                finalContext.put("id", uri);
                return Uni.createFrom().item(finalContext);
            } else return Uni.createFrom().item(new HashMap<>());
        }catch (Exception e){
            return Uni.createFrom().item(new HashMap<>());
        }
    }

    public Uni<RestResponse<Object>> createOrGetCache(String uri, Boolean details, Boolean loadNewCache) {
        Set<Object> cacheSet = cache.as(CaffeineCache.class).keySet();
        if (!loadNewCache && !cacheSet.contains(uri))
            return Uni.createFrom().item(RestResponse.notFound());
        if (details)
            return load(uri).onItem().transform(RestResponse::ok);
        else
            return load(uri).onItem()
                    .transform(map -> {
                        if(map==null || map.isEmpty()) return RestResponse.notFound();
                        else return RestResponse.ok(map.get("body"));
                    });
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
