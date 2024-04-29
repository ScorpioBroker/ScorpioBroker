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
	@PostConstruct
	void setup() {
		atContextUrl = microServiceUtils.getGatewayURL().toString()+"/ngsi-ld/v1/jsonldContexts/";
	}
	String atContextUrl;
	public Uni<RestResponse<Object>> getById(String id, Boolean details) {
		String sql = """
                with a as(select * from contexts WHERE id=$1)
                update contexts set lastusage = now(), numberofhits = numberofhits+1 WHERE id=$1 returning (select to_jsonb(a) from a)""";
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery(sql).execute(Tuple.of(id)).onItem().transformToUni(rows -> {
				if (rows.size() > 0) {
					Row row = rows.iterator().next();
					Map<String,Object> rawData = row.getJsonObject(0).getMap();
					Map<String, Object> result = new HashMap<>();
					if (details) {
						result.put(NGSIConstants.LOCAL_ID, rawData.get(NGSIConstants.ID));
						result.put(NGSIConstants.KIND, rawData.get(NGSIConstants.KIND));
						result.put(NGSIConstants.NUMBER_OF_HITS, rawData.get(NGSIConstants.NUMBER_OF_HITS.toLowerCase()));
						result.put(NGSIConstants.LAST_USAGE, rawData.get(NGSIConstants.LAST_USAGE.toLowerCase()));
						result.put(NGSIConstants.URL, atContextUrl
								+ URLEncoder.encode(rawData.get(NGSIConstants.ID).toString(), StandardCharsets.UTF_8));
						result.put(NGSIConstants.BODY,rawData.get(NGSIConstants.BODY));
						result.put(NGSIConstants.CREATEDAT,rawData.get(NGSIConstants.CREATEDAT.toLowerCase()));
						return Uni.createFrom().item(RestResponse.ok(result));
					} else
						return Uni.createFrom().item(RestResponse.ok(rawData.get(NGSIConstants.BODY)));

				} else
					return Uni.createFrom().item(RestResponse.notFound());
			});
		});
	}

	public Uni<RestResponse<Object>> hostContext(Map<String, Object> payload) {
		String sql = "INSERT INTO public.contexts (id, body, kind) values($1, $2, 'Hosted') returning id";
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery(sql).execute(Tuple.of("urn:" + UUID.randomUUID(), new JsonObject(payload)))
					.onItem().transformToUni(rows -> {
						if (rows.size() > 0) {
                            try {
                                return Uni.createFrom()
                                        .item(RestResponseBuilderImpl.create(201)
                                                .location(new URI(atContextUrl
                                                        + rows.iterator().next().getString(0)))
                                                .build());
                            } catch (URISyntaxException e) {
                                throw new RuntimeException(e);
                            }
                        } else
							return Uni.createFrom().failure(new Throwable("Server Error"));
					});
		});

	}

	public Uni<RestResponse<Object>> deleteById(String id) {
		String sql = "DELETE FROM public.contexts WHERE id=$1 RETURNING id";
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery(sql).execute(Tuple.of(id)).onItem().transformToUni(rows -> {
				if (rows.size() > 0)
					return Uni.createFrom().item(RestResponseBuilderImpl.noContent().build());
				else
					return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
			});
		});
	}

	public Uni<List<Object>> getAllContexts(String kind, Boolean details) {
		StringBuilder sql = new StringBuilder();
		sql.append("Select * from public.contexts ");
		if (kind != null) {
			sql.append("where kind='%s'".formatted(kind.toLowerCase()));
		}
		List<Object> contexts = new ArrayList<>();
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery(sql.toString()).execute().onItem().transform(rows -> {
				rows.forEach(row -> {
					Map<String, Object> result = new HashMap<>();

					if (details) {
						result.put(NGSIConstants.LOCAL_ID, row.getValue(NGSIConstants.ID));
						result.put(NGSIConstants.NUMBER_OF_HITS, row.getValue(NGSIConstants.NUMBER_OF_HITS.toLowerCase()));
						result.put(NGSIConstants.LAST_USAGE, row.getValue(NGSIConstants.LAST_USAGE.toLowerCase()));
						result.put(NGSIConstants.KIND, row.getValue(NGSIConstants.KIND));
						result.put(NGSIConstants.BODY,((JsonObject)row.getJson(NGSIConstants.BODY)).getMap());
						result.put(NGSIConstants.CREATEDAT,row.getLocalDateTime(NGSIConstants.CREATEDAT.toLowerCase()));
						result.put(NGSIConstants.URL, atContextUrl
								+ URLEncoder.encode(row.getValue(NGSIConstants.ID).toString(), StandardCharsets.UTF_8));
						contexts.add(result);
					} else {
						contexts.add(atContextUrl
								+ URLEncoder.encode(row.getValue( NGSIConstants.ID).toString(), StandardCharsets.UTF_8));
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
							if (failure instanceof PgException && ((PgException) failure).getSqlState().equals("23505")) {//already exists
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
