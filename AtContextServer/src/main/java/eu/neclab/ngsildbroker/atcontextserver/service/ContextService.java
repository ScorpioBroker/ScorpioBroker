package eu.neclab.ngsildbroker.atcontextserver.service;

import com.github.jsonldjava.core.JsonLdOptions;
import eu.neclab.ngsildbroker.atcontextserver.cache.ContextCache;
import eu.neclab.ngsildbroker.atcontextserver.dao.ContextDao;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import org.jboss.resteasy.reactive.RestResponse;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
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

	void startup(@Observes StartupEvent event) {
		webClient = WebClient.create(vertx);
	}

	public Uni<Map<String, Object>> getContextById(String id, boolean details) {
		return cache.createOrGetCache(id, details, false).onFailure().recoverWithUni(e -> {
			return dao.getById(id, details);
		});
	}

	public Uni<NGSILDOperationResult> createContextHosted(Map<String, Object> payload) {
		return dao.hostContext(payload).onItem().transform(id -> {
			NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, id,
					AppConstants.INTERNAL_NULL_KEY);
			return result;
		});
	}

	public Uni<Void> deleteById(String id, Boolean reload) {
		boolean isCached = cache.isCached(id);
		if (reload && !isCached) {
			return Uni.createFrom().failure(
					new ResponseException(ErrorType.BadRequestData, "You can't reload anything that is not cached"));
		}
		return dao.deleteById(id).onItemOrFailure().transformToUni((response, fail) -> {
			if (!isCached) {
				if (fail != null) {
					return Uni.createFrom().failure(fail);
				}
				return Uni.createFrom().voidItem();
			}
			if (reload) {
				return cache.reload(id);
			}
			return cache.invalidate(id);
		});
	}

	public Uni<List<Object>> getContexts(String kind, Boolean details) {
		if (kind != null && kind.equalsIgnoreCase(NGSIConstants.CACHED)) {
			return cache.getAllCache(details);
		} else if (kind != null && !kind.equalsIgnoreCase(NGSIConstants.CACHED))
			return dao.getAllContexts(kind, details);
		else
			return cache.getAllCache(details).onItem()
					.transformToUni(list1 -> dao.getAllContexts(kind, details).onItem().transform(list2 -> {
						List<Object> finalList = new ArrayList<>();
						finalList.addAll(list1);
						finalList.addAll(list2);
						return finalList;
					}));
	}

	public Uni<Map<String, Object>> createOrGetCache(String url) {// create cache or implicitly created context
		return cache.createOrGetCache(url, false, true);
	}

	public Uni<RestResponse<Object>> createImplicitly(Map<String, Object> payload) {
		return dao.createContextImpl(payload);

	}
}
