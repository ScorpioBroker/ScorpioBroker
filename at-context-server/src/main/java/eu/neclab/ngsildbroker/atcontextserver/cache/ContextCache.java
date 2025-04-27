package eu.neclab.ngsildbroker.atcontextserver.cache;

import com.github.jsonldjava.core.JsonLdOptions;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@ApplicationScoped
public class ContextCache {
	@CacheName("context")
	Cache cache;

	@ConfigProperty(name = "atcontext.cache.duration", defaultValue = "20m")
	String cacheDurationTime;
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
	Map<String, String> id2LastUsage = new HashMap<>();
	String atContextUrl;
	Duration cacheDuration;

	@PostConstruct
	void init() {
		webClient = WebClient.create(vertx);
		atContextUrl = microServiceUtils.getGatewayURL().toString() + "/ngsi-ld/v1/jsonldContexts/";
		if (!cacheDurationTime.startsWith("PT")) {
			cacheDurationTime = "PT" + cacheDurationTime;
		}
		cacheDuration = Duration.parse(cacheDurationTime);
	}

	public Uni<Map<String, Object>> load(String uri, boolean reload) {
		logger.debug("loading uri " + uri);
		CaffeineCache caffeinCache = cache.as(CaffeineCache.class);
		CompletableFuture<Object> valueFuture = caffeinCache.getIfPresent(uri);
		if (valueFuture != null && !reload) {
			logger.debug("using cache");
			return Uni.createFrom().completionStage(valueFuture).onItem().transformToUni(value -> {
				logger.debug("retrieved cache");
				return Uni.createFrom().item((Map<String, Object>) value);
			});
		}
		logger.debug("loading from server");
		return jsonLdOptions.getDocumentLoader().loadDocument(uri, webClient).onItem().transformToUni(rd -> {
			if (rd.getDocument() instanceof Map<?, ?> map && map.containsKey(NGSIConstants.JSON_LD_CONTEXT)) {
				Map<String, Object> finalContext = new HashMap<>();
				finalContext.put(NGSIConstants.BODY,
						Map.of(NGSIConstants.JSON_LD_CONTEXT, map.get(NGSIConstants.JSON_LD_CONTEXT)));
				finalContext.put(NGSIConstants.KIND, NGSIConstants.CACHED);
				finalContext.put(NGSIConstants.CREATEDAT, SerializationTools.formatter.format(Instant.now()));
				finalContext.put(NGSIConstants.URL, atContextUrl + URLEncoder.encode(uri, StandardCharsets.UTF_8));
				finalContext.put(NGSIConstants.LOCAL_ID, uri);
				cache.as(CaffeineCache.class).put(uri,
						Uni.createFrom().item(finalContext).subscribeAsCompletionStage());
				return Uni.createFrom().item(finalContext);
			} else
				return Uni.createFrom().item(new HashMap<>());

		});

	}

	public boolean isCached(String uri) {
		Set<Object> cacheSet = cache.as(CaffeineCache.class).keySet();
		return cacheSet.contains(uri);
	}

	public Uni<Map<String, Object>> createOrGetCache(String uri, Boolean details, Boolean loadNewCache) {
		logger.debug("Create or cache uri " + uri);
		Set<Object> cacheSet = cache.as(CaffeineCache.class).keySet();
		if (!loadNewCache && !cacheSet.contains(uri)) {
			return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "Context was not found"));
		}
		long hit = id2numberOfHit.getOrDefault(uri, 0L) + 1;
		id2numberOfHit.put(uri, hit);
		String lastUsage = id2LastUsage.get(uri);
		id2LastUsage.put(uri, SerializationTools.formatter.format(Instant.now()));
		if (details) {
			return load(uri, false).onItem().transform(map -> {

				if (lastUsage != null) {
					map.put(NGSIConstants.LAST_USAGE, lastUsage);
				}
				map.put(NGSIConstants.EXPIRES_AT,
						SerializationTools.formatter.format(Instant.now().plus(cacheDuration)));
				map.put(NGSIConstants.NUMBER_OF_HITS, hit - 1);
				return map;
			});
		} else {
			return load(uri, false).onItemOrFailure().transformToUni((map, fail) -> {
				if (fail != null || map == null || map.isEmpty()) {
					return Uni.createFrom().failure(new ResponseException(ErrorType.LdContextNotAvailable));
				} else {
					return Uni.createFrom().item((Map<String, Object>) map.get(NGSIConstants.BODY));
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

					cachedItem.put(NGSIConstants.EXPIRES_AT,
							SerializationTools.formatter.format(Instant.now().plus(cacheDuration)));
					long hit = id2numberOfHit.getOrDefault(key.toString(), 0L);
					cachedItem.put(NGSIConstants.NUMBER_OF_HITS, hit);
					id2numberOfHit.put(key.toString(), hit + 1);
					cachedItem.put(NGSIConstants.LAST_USAGE, id2LastUsage.get(key.toString()));
				} else
					list.add(cachedItem.get(NGSIConstants.URL));
			} catch (Exception ignored) {

			}
		});
		return Uni.createFrom().item(list);
	}

	
	public Uni<Void> reload(String uri) {
		logger.debug("reloading cache for uri " + uri);
		return load(uri, true).onItem().transformToUni(res -> Uni.createFrom().voidItem());
	}

	@CacheInvalidate(cacheName = "context")
	public Uni<Void> invalidate(@CacheKey String uri) {
		id2numberOfHit.remove(uri);
		id2LastUsage.remove(uri);
		return Uni.createFrom().voidItem();
	}

//	@CacheInvalidateAll(cacheName = "context")
//	@Scheduled(every = "${atcontext.cache.duration}", identity = "cacheDuration")
//	public void invalidateAll() {
//		id2numberOfHit.clear();
//		id2LastUsage.clear();
//	}
}
