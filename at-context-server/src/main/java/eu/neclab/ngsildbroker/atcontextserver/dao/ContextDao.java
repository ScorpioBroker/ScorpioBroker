package eu.neclab.ngsildbroker.atcontextserver.dao;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.jaxrs.RestResponseBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.pgclient.PgException;

@ApplicationScoped
public class ContextDao {
	@Inject
	ClientManager clientManager;

	@Inject
	MicroServiceUtils microServiceUtils;

	private static Logger logger = LoggerFactory.getLogger(ContextDao.class);

	String atContextUrl;

	@PostConstruct
	void setup() {
		atContextUrl = microServiceUtils.getGatewayURL().toString() + "/ngsi-ld/v1/jsonldContexts/";
	}

	public Uni<Map<String, Object>> getById(String id, Boolean details) {
		String sql = """
				with a as(select * from contexts WHERE id=$1),
				b as (update contexts set lastusage = now(), numberofhits = numberofhits+1 WHERE id=$1)
				select * from a""";
		logger.debug(sql);
		logger.debug(id);
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery(sql).execute(Tuple.of(id)).onItem().transformToUni(rows -> {
				if (rows.size() > 0) {
					Row row = rows.iterator().next();

					Map<String, Object> result = new HashMap<>();
					if (details) {
						String localId = row.getString(NGSIConstants.ID);
						result.put(NGSIConstants.LOCAL_ID, localId);
						result.put(NGSIConstants.KIND, row.getString(NGSIConstants.KIND));
						result.put(NGSIConstants.NUMBER_OF_HITS,
								row.getLong(NGSIConstants.NUMBER_OF_HITS.toLowerCase()));
						String lastUsage = row.getString(NGSIConstants.LAST_USAGE.toLowerCase());
						String createdAt = row.getString(NGSIConstants.CREATEDAT.toLowerCase());
						if (lastUsage != null) {
							result.put(NGSIConstants.LAST_USAGE, lastUsage + 'Z');
						}
						result.put(NGSIConstants.URL,
								atContextUrl + URLEncoder.encode(localId, StandardCharsets.UTF_8));
						result.put(NGSIConstants.BODY, row.getJsonObject(NGSIConstants.BODY).getMap());
						result.put(NGSIConstants.CREATEDAT, createdAt + 'Z');
						return Uni.createFrom().item(result);
					} else
						return Uni.createFrom().item(row.getJsonObject(NGSIConstants.BODY).getMap());

				} else
					return Uni.createFrom()
							.failure(new ResponseException(ErrorType.NotFound, "The context was not found"));
			});
		});
	}

	public Uni<String> hostContext(Map<String, Object> payload) {
		String sql = "INSERT INTO contexts (id, body, kind) values($1, $2, 'Hosted') returning id";
		String id = "urn:" + UUID.randomUUID();
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery(sql).execute(Tuple.of(id, new JsonObject(payload))).onFailure()
					.recoverWithUni(e -> {

						if (e instanceof PgException pge) {

							MicroServiceUtils.logPGE(pge, logger);
							if (pge.getSqlState().equals(AppConstants.SQL_NOT_FOUND)) {
								return Uni.createFrom()
										.failure(new ResponseException(ErrorType.NotFound, id + " not found"));
							}
							if (pge.getSqlState().startsWith("SB")) {
								return Uni.createFrom().failure(
										new ResponseException(ErrorType.BadRequestData, pge.getErrorMessage()));
							}
						}
						logger.debug("database exception", e);
						return Uni.createFrom().failure(e);
					}).onItem().transformToUni(rows -> {
						if (rows.size() > 0) {
							return Uni.createFrom().item(rows.iterator().next().getString(0));
						} else {
							return Uni.createFrom().failure(
									new ResponseException(ErrorType.InternalError, "An unexcpected error happened"));
						}
					});
		});

	}

	public Uni<Void> deleteById(String id) {
		String sql = "DELETE FROM contexts WHERE id=$1 RETURNING id";
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery(sql).execute(Tuple.of(id)).onItem().transformToUni(rows -> {
				if (rows.size() > 0) {
					return Uni.createFrom().voidItem();
				} else {
					return Uni.createFrom()
							.failure(new ResponseException(ErrorType.NotFound, "@Context was not found"));
				}
			});
		});
	}

	public Uni<List<Object>> getAllContexts(String kind, Boolean details) {
		StringBuilder sql = new StringBuilder();
		sql.append("Select * from contexts ");
		if (kind != null) {
			sql.append("where kind='%s'".formatted(kind));
		}
		List<Object> contexts = new ArrayList<>();
		logger.debug(sql.toString());
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery(sql.toString()).execute().onItem().transform(rows -> {
				rows.forEach(row -> {
					Map<String, Object> result = new HashMap<>();

					if (details) {
						result.put(NGSIConstants.LOCAL_ID, row.getValue(NGSIConstants.ID));
						result.put(NGSIConstants.NUMBER_OF_HITS,
								row.getValue(NGSIConstants.NUMBER_OF_HITS.toLowerCase()));
						result.put(NGSIConstants.LAST_USAGE, row.getValue(NGSIConstants.LAST_USAGE.toLowerCase()));
						result.put(NGSIConstants.KIND, row.getValue(NGSIConstants.KIND));
						result.put(NGSIConstants.BODY, ((JsonObject) row.getJson(NGSIConstants.BODY)).getMap());
						result.put(NGSIConstants.CREATEDAT,
								row.getLocalDateTime(NGSIConstants.CREATEDAT.toLowerCase()));
						result.put(NGSIConstants.URL, atContextUrl
								+ URLEncoder.encode(row.getValue(NGSIConstants.ID).toString(), StandardCharsets.UTF_8));
						contexts.add(result);
					} else {
						contexts.add(atContextUrl
								+ URLEncoder.encode(row.getValue(NGSIConstants.ID).toString(), StandardCharsets.UTF_8));
					}

				});
				return contexts;
			});
		});

	}

	public Uni<RestResponse<Object>> createContextImpl(Map<String, Object> payload) {
		java.security.MessageDigest md;
		try {
			md = java.security.MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
		byte[] array = md.digest(payload.toString().getBytes());
		StringBuilder sb = new StringBuilder();
		for (byte b : array) {
			sb.append(Integer.toHexString((b & 0xFF) | 0x100), 1, 3);
		}
		String id = "urn:" + sb;
		String sql = "INSERT INTO public.contexts (id, body, kind) values($1, $2, 'ImplicitlyCreated') returning id";
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery(sql).execute(Tuple.of(id, new JsonObject(payload))).onItemOrFailure()
					.transform((rows, failure) -> {
						if (failure != null) {
							if (failure instanceof PgException
									&& ((PgException) failure).getSqlState().equals("23505")) {// already exists
								return RestResponse.ok(id);
							} else {
								return RestResponse.status(RestResponse.Status.INTERNAL_SERVER_ERROR,
										failure.getMessage());
							}

						}
						if (rows.size() > 0) {
							return RestResponse.ok(rows.iterator().next().getString(0));
						} else {
							return RestResponse.status(RestResponse.Status.INTERNAL_SERVER_ERROR);
						}
					});
		});
	}
}
