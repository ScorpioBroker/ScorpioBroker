package eu.neclab.ngsildbroker.atcontextserver.dao;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.jaxrs.RestResponseBuilderImpl;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
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

	@PostConstruct
	void setup() {

	}

	public Uni<RestResponse<Object>> getById(String id, Boolean details) {
		String sql;
		if (details) {
			sql = "SELECT * FROM public.contexts WHERE id=$1";
		} else {
			sql = "SELECT body FROM public.contexts WHERE id=$1";
		}
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery(sql).execute(Tuple.of(id)).onItem().transformToUni(rows -> {
				if (rows.size() > 0) {
					Row row = rows.iterator().next();
					Map<String, Object> map = row.toJson().getMap();
					if (details) {
						map.put(row.getColumnName(1), ((JsonObject) map.get(row.getColumnName(1))).getMap());
						map.put("url", "http://localhost:9090/" + NGSIConstants.JSONLD_CONTEXTS + map.get("id"));
						return Uni.createFrom().item(RestResponse.ok(map));
					} else
						return Uni.createFrom().item(RestResponse.ok(row.getJson("body")));

				} else
					return Uni.createFrom().item(RestResponse.notFound());
			});
		});
	}

	public Uni<RestResponse<Object>> hostContext(Map<String, Object> payload) {
		String sql = "INSERT INTO public.contexts (id, body, kind) values($1, $2, 'hosted') ON CONFLICT DO NOTHING returning id";
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery(sql).execute(Tuple.of("urn:" + payload.hashCode(), new JsonObject(payload)))
					.onItem().transformToUni(rows -> {
						if (rows.size() > 0) {
							return Uni.createFrom()
									.item(RestResponseBuilderImpl.create(201)
											.entity(new JsonObject("{\"url\":\"" + "http://localhost:9090/"
													+ NGSIConstants.JSONLD_CONTEXTS
													+ rows.iterator().next().getString(0) + "\"}"))
											.build());
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
		if (details) {
			sql.append("Select * from public.contexts ");
		} else {
			sql.append("Select id from public.contexts ");
		}
		if (kind != null) {
			sql.append("where kind='%s'".formatted(kind.toLowerCase()));
		}
		List<Object> contexts = new ArrayList<>();
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery(sql.toString()).execute().onItem().transform(rows -> {
				rows.forEach(i -> {
					Map<String, Object> map = i.toJson().getMap();

					if (details) {
						map.put(i.getColumnName(1), ((JsonObject) map.get(i.getColumnName(1))).getMap());
						map.put("url", "http://localhost:9090/" + NGSIConstants.JSONLD_CONTEXTS
								+ URLEncoder.encode(map.get("id").toString(), StandardCharsets.UTF_8));
						contexts.add(map);
					} else {
						contexts.add("http://localhost:9090/" + NGSIConstants.JSONLD_CONTEXTS
								+ URLEncoder.encode(map.get("id").toString(), StandardCharsets.UTF_8));
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
		String sql = "INSERT INTO public.contexts (id, body, kind) values($1, $2, 'implicitlycreated') returning id";
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery(sql).execute(Tuple.of(id, new JsonObject(payload))).onItemOrFailure()
					.transform((rows, failure) -> {
						if (failure != null) {
							if (failure instanceof PgException && ((PgException) failure).getCode().equals("23505")) {
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
