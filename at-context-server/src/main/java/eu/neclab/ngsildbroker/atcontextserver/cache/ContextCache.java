package eu.neclab.ngsildbroker.atcontextserver.cache;

import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.RemoteDocument;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheInvalidate;
import io.quarkus.cache.CacheInvalidateAll;
import io.quarkus.cache.CacheKey;
import io.quarkus.cache.CacheName;
import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CaffeineCache;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import org.jboss.resteasy.reactive.RestResponse;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


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
            if (rd.getDocument() instanceof Map<?, ?> map && map.containsKey(NGSIConstants.JSON_LD_CONTEXT)) {
                Map<String, Object> finalContext = new HashMap<>();
                finalContext.put(NGSIConstants.BODY, Map.of(NGSIConstants.JSON_LD_CONTEXT,map.get(NGSIConstants.JSON_LD_CONTEXT)));
                finalContext.put(NGSIConstants.KIND, NGSIConstants.CACHED);
                finalContext.put(NGSIConstants.CREATEDAT, new Timestamp(System.currentTimeMillis()));
                finalContext.put(NGSIConstants.URL, uri);
                finalContext.put(NGSIConstants.ID, uri);
                return Uni.createFrom().item(finalContext);
            } else
                return Uni.createFrom().item(new HashMap<>());
        } catch (Exception e) {
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
                        if (map == null || map.isEmpty())
                            return RestResponse.notFound();
                        else
                            return RestResponse.ok(map.get(NGSIConstants.BODY));
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
                    list.add(cachedItem.get(NGSIConstants.URL));
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
    }
}
