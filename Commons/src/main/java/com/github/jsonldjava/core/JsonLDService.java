package com.github.jsonldjava.core;

import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;

@Singleton
public class JsonLDService {

	private Context coreContext;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContextUrl;

	@Inject
	ClientManager clientManager;
	@Inject
	MicroServiceUtils microServiceUtils;
	@Inject
	Vertx vertx;

	WebClient webClient;
	String atContextUrl;

	@PostConstruct
	void setup() {
		WebClientOptions options = new WebClientOptions();
		atContextUrl = microServiceUtils.getContextServerURL().toString();
		this.webClient = WebClient.create(vertx, options);
		this.coreContext = clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem()
				.transformToUni(client -> {
					return client
							.preparedQuery(
									"SELECT body FROM contexts WHERE id='" + AppConstants.INTERNAL_NULL_KEY + "'")
							.execute().onItem().transform(rows -> {
								return rows.iterator().next().getJsonObject(0).getMap();
							});
				}).onItem().transformToUni(coreContextMap -> {
					return new Context(new JsonLdOptions(JsonLdOptions.JSON_LD_1_1))
							.parse(coreContextMap.get("@context"), false, webClient, atContextUrl).onItem()
							.transform(coreContext -> {
								// this.coreContext = coreContext;
								coreContext.getTermDefinition("features").remove("@container");
								coreContext.getInverse();
								return coreContext;
							});
					// this explicitly removes the features term from the core as it is a commonly
					// used term and we don't need the geo json definition

				}).await().indefinitely();
		JsonLdProcessor.init(coreContextUrl, coreContext, atContextUrl);

	}

	// This is needed so that @postconstruct runs on the startup thread and not on a
	// worker thread later on
	void startup(@Observes StartupEvent event) {
	}

	public Context getCoreContextClone() {
		Context clone = coreContext.clone();
		clone.inverse = null;
		return clone;
	}

	public Uni<Map<String, Object>> compact(Object input, Object context, JsonLdOptions opts) {
		return JsonLdProcessor.compact(input, context, opts, webClient);
	}

	public Uni<Map<String, Object>> compact(Object input, Object context, Context activeCtx, JsonLdOptions opts,
			int endPoint) {
		return JsonLdProcessor.compact(input, context, activeCtx, opts, endPoint, webClient);
	}

	public Uni<Map<String, Object>> compact(Object input, Object context, Context activeCtx, JsonLdOptions opts,
			int endPoint, Set<String> options, LanguageQueryTerm langQuery) {
		return JsonLdProcessor.compact(input, context, activeCtx, opts, endPoint, options, langQuery, webClient);
	}

	public Uni<List<Object>> expand(List<Object> contextLinks, Object input, JsonLdOptions opts, int payloadType,
			boolean atContextAllowed) {
		return JsonLdProcessor.expand(contextLinks, input, opts, payloadType, atContextAllowed, webClient);
	}

	public Uni<List<Object>> expand(Context activeCtx, Object input, JsonLdOptions opts, int payloadType,
			boolean atContextAllowed) {
		return JsonLdProcessor.expand(activeCtx, input, opts, payloadType, atContextAllowed, webClient);
	}

	public Uni<List<Object>> expand(Object input) {
		return JsonLdProcessor.expand(input, webClient);
	}

	public Context getCoreContext() {
		return coreContext;
	}

	public Uni<Context> parse(Object headerContext) {
		try {
			return getCoreContextClone().parse(headerContext, true, webClient, atContextUrl).onFailure()
					.recoverWithUni(e -> {
						return Uni.createFrom().failure(new ResponseException(ErrorType.LdContextNotAvailable,
								"Failed to retrieve remote context because " + e.getLocalizedMessage()));
					});
		} catch (Exception e) {
			return Uni.createFrom().failure(new ResponseException(ErrorType.LdContextNotAvailable,
					"Failed to retrieve remote context because " + e.getLocalizedMessage()));
		}
	}

	public Uni<Context> parsePure(Object headerContext) {
		return new Context().parse(headerContext, false, webClient, atContextUrl);
	}

	public Uni<Object> toRDF(Object entity) {
		return JsonLdProcessor.toRDF(entity, webClient);
	}

}
