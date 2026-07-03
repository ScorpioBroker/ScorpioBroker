package eu.neclab.ngsildbroker.entityhandler.controller;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.jaxrs.RestResponseBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HttpHeaders;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService.BulkResult;
import io.quarkus.runtime.Startup;
import io.smallrye.common.annotation.Blocking;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;

/**
 * Custom high-throughput bulk-insert endpoint. Streams the upload (NDJSON or a
 * JSON array) without buffering the whole body, expands entities in chunks and
 * overwrites them in the database via COPY (see
 * {@link EntityService#bulkInsertChunk}). Intended for loads of 100k - millions
 * of entities.
 *
 * Runs {@code @Blocking} on a worker thread: the request {@link InputStream} is
 * consumed incrementally (it is not fully buffered by the framework), entities
 * are assembled into copy-batches and each batch is expanded + stored before the
 * next is read, so memory stays bounded. Flow stays controller -> service -> DAO.
 */
@ApplicationScoped
@Startup
@Path("/scorpio/v1")
public class BulkController {

	private static final Logger logger = LoggerFactory.getLogger(BulkController.class);

	private enum Format {
		NDJSON, ARRAY
	}

	@Inject
	EntityService entityService;

	@Inject
	ObjectMapper objectMapper;

	@ConfigProperty(name = "scorpio.entity.bulk.copy.batch")
	int copyBatchSize;

	@POST
	@Path("/bulkinsert")
	@Blocking
	public RestResponse<Object> bulkInsert(HttpServerRequest request, @QueryParam("trigger") String triggerS,
			InputStream body) {
		String tenant = HttpUtils.getTenant(request);
		try {
			boolean trigger = HttpUtils.parseBoolean(triggerS);
			// Permissive for bulk ingest: honor a shared @context from the Link header.
			// Per-entity @context that only references NGSI-LD core URLs is stripped before
			// expand (see stripRedundantCoreContext).
			List<Object> atContext = HttpUtils.getAtContext(request);
			boolean atContextAllowed = true;
			BulkResult result = new BulkResult();

			BufferedInputStream in = new BufferedInputStream(body);
			Format format = detectFormat(request, in);
			if (format == Format.NDJSON) {
				processNdjson(in, tenant, atContext, atContextAllowed, trigger, result);
			} else {
				processArray(in, tenant, atContext, atContextAllowed, trigger, result);
			}
			return buildResponse(result);
		} catch (Exception e) {
			logger.error("Bulk insert failed", e);
			return HttpUtils.handleControllerExceptions(e, tenant);
		}
	}

	private void processNdjson(InputStream in, String tenant, List<Object> atContext, boolean atContextAllowed,
			boolean trigger, BulkResult result) throws Exception {
		BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
		List<Map<String, Object>> batch = new ArrayList<>(copyBatchSize);
		String line;
		while ((line = reader.readLine()) != null) {
			String text = line.trim();
			if (text.isEmpty()) {
				continue;
			}
			try {
				Map<String, Object> entity = new JsonObject(text).getMap();
				stripRedundantCoreContext(entity);
				batch.add(entity);
			} catch (Exception e) {
				logger.warn("Skipping malformed NDJSON line in bulk insert: {}", e.getMessage());
				continue;
			}
			if (batch.size() >= copyBatchSize) {
				flush(batch, tenant, atContext, atContextAllowed, trigger, result);
			}
		}
		flush(batch, tenant, atContext, atContextAllowed, trigger, result);
	}

	private void processArray(InputStream in, String tenant, List<Object> atContext, boolean atContextAllowed,
			boolean trigger, BulkResult result) throws Exception {
		List<Map<String, Object>> batch = new ArrayList<>(copyBatchSize);
		try (JsonParser parser = objectMapper.getFactory().createParser(in)) {
			JsonToken token = parser.nextToken();
			if (token == JsonToken.START_ARRAY) {
				while (parser.nextToken() == JsonToken.START_OBJECT) {
					batch.add(readObject(parser));
					if (batch.size() >= copyBatchSize) {
						flush(batch, tenant, atContext, atContextAllowed, trigger, result);
					}
				}
			} else if (token == JsonToken.START_OBJECT) {
				// tolerate a single object posted as application/json
				batch.add(readObject(parser));
			}
		}
		flush(batch, tenant, atContext, atContextAllowed, trigger, result);
	}

	private Map<String, Object> readObject(JsonParser parser) throws Exception {
		@SuppressWarnings("unchecked")
		Map<String, Object> map = parser.readValueAs(Map.class);
		stripRedundantCoreContext(map);
		return map;
	}

	/**
	 * Drops per-entity {@code @context} when it only references NGSI-LD core context
	 * URLs ({@link NGSIConstants#CORE_CONTEXT_URLS}). The broker already loads the core
	 * context; keeping it on each entity forces a redundant clone/parse on every expand.
	 */
	private void stripRedundantCoreContext(Map<String, Object> entity) {
		Object context = entity.get(NGSIConstants.JSON_LD_CONTEXT);
		if (context == null) {
			return;
		}
		if (usesOnlyCoreContextUrls(context)) {
			entity.remove(NGSIConstants.JSON_LD_CONTEXT);
		}
	}

	private boolean usesOnlyCoreContextUrls(Object context) {
		if (context instanceof String) {
			return NGSIConstants.CORE_CONTEXT_URLS.contains(context);
		}
		if (context instanceof List<?> list) {
			if (list.isEmpty()) {
				return false;
			}
			for (Object item : list) {
				if (!(item instanceof String) || !NGSIConstants.CORE_CONTEXT_URLS.contains(item)) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	/**
	 * Stores the assembled batch via the service (expand + COPY-overwrite) and waits
	 * for it before reading the next batch. Blocking await is intentional here - we
	 * are already on a worker thread, and it gives natural backpressure on the
	 * upload.
	 */
	private void flush(List<Map<String, Object>> batch, String tenant, List<Object> atContext, boolean atContextAllowed,
			boolean trigger, BulkResult result) {
		if (batch.isEmpty()) {
			return;
		}
		List<Map<String, Object>> chunk = new ArrayList<>(batch);
		batch.clear();
		long start = System.currentTimeMillis();
		entityService.bulkInsertChunk(tenant, chunk, atContext, atContextAllowed, trigger, result).await()
				.indefinitely();
		if (logger.isDebugEnabled()) {
			logger.debug("bulk flush took {}ms ({} entities)", System.currentTimeMillis() - start, chunk.size());
		}
	}

	private Format detectFormat(HttpServerRequest request, BufferedInputStream in) throws Exception {
		String contentType = request.getHeader(HttpHeaders.CONTENT_TYPE);
		if (contentType != null) {
			contentType = contentType.toLowerCase();
			if (contentType.contains("ndjson") || contentType.contains("json-seq")) {
				return Format.NDJSON;
			}
			if (contentType.contains("json")) {
				return Format.ARRAY;
			}
		}
		// Sniff the first non-whitespace byte: '[' => JSON array, otherwise NDJSON.
		in.mark(8192);
		int read;
		Format sniffed = Format.NDJSON;
		while ((read = in.read()) != -1) {
			char c = (char) read;
			if (!Character.isWhitespace(c)) {
				sniffed = (c == '[') ? Format.ARRAY : Format.NDJSON;
				break;
			}
		}
		in.reset();
		return sniffed;
	}

	private RestResponse<Object> buildResponse(BulkResult result) {
		Map<String, Object> body = new LinkedHashMap<>();
		body.put("received", result.getReceived());
		body.put("inserted", result.getInserted());
		body.put("expandFailed", new ArrayList<>(result.getExpandFailedIds()));
		return new RestResponseBuilderImpl<Object>().status(200).type(AppConstants.NGB_APPLICATION_JSON)
				.entity(new JsonObject(body).encode()).build();
	}

}
