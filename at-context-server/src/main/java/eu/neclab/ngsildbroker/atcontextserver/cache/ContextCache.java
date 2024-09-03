package eu.neclab.ngsildbroker.atcontextserver.cache;

import com.github.jsonldjava.core.JsonLdOptions;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheInvalidate;
import io.quarkus.cache.CacheInvalidateAll;
import io.quarkus.cache.CacheKey;
import io.quarkus.cache.CacheName;
import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CaffeineCache;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.scheduler.Scheduler;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;

import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@ApplicationScoped
public class ContextCache {
    @CacheName("context")
    Cache cache;

    @ConfigProperty(name = "atcontext.cache.duration", defaultValue = "20m")
    String cacheDuration;
    JsonLdOptions jsonLdOptions = new JsonLdOptions();
    private static Logger logger = LoggerFactory.getLogger(ContextCache.class);

    @Inject
    Vertx vertx;

    @Inject
    Scheduler scheduler;
    private WebClient webClient;
    @Inject
    MicroServiceUtils microServiceUtils;

    Map<String, Long> id2numberOfHit = new HashMap<>();
    Map<String, Object> id2LastUsage = new HashMap<>();
    String atContextUrl;

    @PostConstruct
    void init() {
        webClient = WebClient.create(vertx);
        atContextUrl = microServiceUtils.getGatewayURL().toString() + "/ngsi-ld/v1/jsonldContexts/";
    }

    @CacheResult(cacheName = "context")
    public Uni<Map<String, Object>> load(String uri) {
        logger.debug("loading uri " + uri);
        return jsonLdOptions.getDocumentLoader().loadDocument(uri, webClient).onItem().transformToUni(rd -> {
            if (rd.getDocument() instanceof Map<?, ?> map && map.containsKey(NGSIConstants.JSON_LD_CONTEXT)) {
                Map<String, Object> finalContext = new HashMap<>();
                finalContext.put(NGSIConstants.BODY,
                        Map.of(NGSIConstants.JSON_LD_CONTEXT, map.get(NGSIConstants.JSON_LD_CONTEXT)));
                finalContext.put(NGSIConstants.KIND, NGSIConstants.CACHED);
                finalContext.put(NGSIConstants.CREATEDAT, new Timestamp(System.currentTimeMillis()));
                finalContext.put(NGSIConstants.URL, atContextUrl + URLEncoder.encode(uri, StandardCharsets.UTF_8));
                finalContext.put(NGSIConstants.LOCAL_ID, uri);
                return Uni.createFrom().item(finalContext);
            } else
                return Uni.createFrom().item(new HashMap<>());

        });
    }

    public Uni<RestResponse<Object>> createOrGetCache(String uri, Boolean details, Boolean loadNewCache) {
        logger.debug("Create or cache uri " + uri);
        Set<Object> cacheSet = cache.as(CaffeineCache.class).keySet();
        if (!loadNewCache && !cacheSet.contains(uri)) {
            return Uni.createFrom().item(RestResponse.notFound());
        }
        long hit = id2numberOfHit.getOrDefault(uri,0L) + 1;
        id2numberOfHit.put(uri,hit);
        Object lastUsage = id2LastUsage.get(uri);
        id2LastUsage.put(uri,new Timestamp(System.currentTimeMillis()));
        if (details) {
            return load(uri).onItem().transform(map -> {
                Instant expiresAt = scheduler.getScheduledJob("cacheDuration").getNextFireTime();
                map.put(NGSIConstants.LAST_USAGE,lastUsage);
                map.put(NGSIConstants.EXPIRES_AT, expiresAt.toString());
                map.put(NGSIConstants.NUMBER_OF_HITS,hit);
                return RestResponse.ok(map);
            });
        } else {
            return load(uri).onItemOrFailure().transform((map, fail) -> {
                if (fail != null || map == null || map.isEmpty()) {
                    return RestResponse.status(Response.Status.SERVICE_UNAVAILABLE);
                }
                else {
                    return RestResponse.ok(map.get(NGSIConstants.BODY));
                }
            });
        }
    }

    public Uni<List<Object>> getAllCache(Boolean details) {
        Set<Object> set = cache.as(CaffeineCache.class).keySet();
        List<Object> list = new ArrayList<>();
        set.forEach(key -> {
            try {
                @SuppressWarnings("unchecked")
				Map<String, Object> cachedItem = (Map<String, Object>) cache.as(CaffeineCache.class).getIfPresent(key)
                        .get();
                if (details) {
                    list.add(cachedItem);
                    Instant expiresAt = scheduler.getScheduledJob("cacheDuration").getNextFireTime();
                    cachedItem.put(NGSIConstants.EXPIRES_AT, expiresAt.toString());
                    long hit = id2numberOfHit.getOrDefault(key.toString(),0L);
                    cachedItem.put(NGSIConstants.NUMBER_OF_HITS,hit);
                    cachedItem.put(NGSIConstants.LAST_USAGE,id2LastUsage.get(key.toString()));
                } else
                    list.add(cachedItem.get(NGSIConstants.URL));
            } catch (Exception ignored) {

            }
        });
        return Uni.createFrom().item(list);
    }

    @CacheInvalidate(cacheName = "context")
    public Uni<RestResponse<Object>> reload(@CacheKey String uri) {
        logger.debug("reloading cache for uri " + uri);
        return load(uri).onItem().transform(res -> RestResponse.ok());
    }

    @CacheInvalidate(cacheName = "context")
    public Uni<RestResponse<Object>> invalidate(@CacheKey String uri) {
        return Uni.createFrom().item(RestResponse.noContent());
    }

    @CacheInvalidateAll(cacheName = "context")
    @Scheduled(every = "${atcontext.cache.duration}", identity = "cacheDuration")
    public void invalidateAll() {
        id2numberOfHit.clear();
        id2LastUsage.clear();
    }
}
