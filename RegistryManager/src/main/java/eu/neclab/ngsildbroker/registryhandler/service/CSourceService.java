package eu.neclab.ngsildbroker.registryhandler.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Sets;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.registryhandler.controller.RegistryController;
import eu.neclab.ngsildbroker.registryhandler.repository.CSourceDAO;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.pgclient.PgException;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Singleton
public class CSourceService {
	private final static Logger logger = LoggerFactory.getLogger(RegistryController.class);
	List<String> scorpioFedList = ConfigProvider.getConfig().getValues("scorpio.federation", String.class);
	Map<String, Map<String, String>> fedMap = new HashMap<>();
	@Inject
	MicroServiceUtils microServiceUtils;

	@Inject
	CSourceDAO cSourceInfoDAO;

	@Inject
	JsonLDService ldService;

	@Inject
	@Channel(AppConstants.REGISTRY_CHANNEL)
	@Broadcast
	MutinyEmitter<String> emitter;

	@ConfigProperty(name = "scorpio.federation.registrationtype", defaultValue = "types")
	String AUTO_REG_MODE;

	@ConfigProperty(name = "scorpio.federation.hosts", defaultValue = " ")
	String FED_BROKERS_CONFIG;

	String[] FED_BROKERS;

	@ConfigProperty(name = "scorpio.topics.registry")
	String CSOURCE_TOPIC;

	@ConfigProperty(name = "scorpio.directDB", defaultValue = "true")
	boolean directDB;

	@Inject
	Vertx vertx;
	
	@Inject
	ObjectMapper objectMapper;

	@ConfigProperty(name = "scorpio.messaging.maxSize")
	int messageSize;

	Map<String, Object> myRegistryInformation;

	private WebClient webClient;

	@PostConstruct
	void setup() {
		this.webClient = WebClient.create(vertx);
		if (FED_BROKERS_CONFIG.isBlank()) {
			FED_BROKERS = new String[0];
		} else {
			FED_BROKERS = FED_BROKERS_CONFIG.split(",");
		}

		if (scorpioFedList == null)
			return;
		for (int i = 0; i < scorpioFedList.size(); i++) {
			Map<String, String> details = new HashMap<>();
			details.put("url", ConfigProvider.getConfig().getValue("scorpio.federation[" + i + "].url", String.class));
			details.put("sourcetenant",
					ConfigProvider.getConfig().getValue("scorpio.federation[" + i + "].sourcetenant", String.class));
			details.put("targettenant",
					ConfigProvider.getConfig().getValue("scorpio.federation[" + i + "].targettenant", String.class));
			details.put("regtype",
					ConfigProvider.getConfig().getValue("scorpio.federation[" + i + "].regtype", String.class));
			fedMap.put(scorpioFedList.get(i), details);
		}

	}

	public Uni<NGSILDOperationResult> createRegistration(String tenant, Map<String, Object> registration) {
		CreateCSourceRequest request;
		String id;
		Object idObj = registration.get(NGSIConstants.JSON_LD_ID);
		if (idObj == null) {
			id = EntityTools.generateUniqueRegId(registration);
			registration.put(NGSIConstants.JSON_LD_ID, id);
		}
		try {
			request = new CreateCSourceRequest(tenant, registration);
		} catch (Exception e) {
			return Uni.createFrom().failure(e);
		}
		return cSourceInfoDAO.createRegistration(request).onItem().transform(rowset -> {
			MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, emitter, objectMapper);
			NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.OPERATION_CREATE_REGISTRATION,
					(String) registration.get(NGSIConstants.JSON_LD_ID));
			result.addSuccess(new CRUDSuccess(null, null, request.getId(), Sets.newHashSet()));
			return result;
		}).onFailure().recoverWithUni(e -> {
			ErrorType error = ErrorType.InternalError;
			String errorMsg = e.getMessage();
			if (e instanceof PgException) {
				if (((PgException) e).getCode().equals("23505")) {
					error = ErrorType.AlreadyExists;
					errorMsg = "Registration already exists";
				}
			}
			e.printStackTrace();
			return Uni.createFrom().failure(new ResponseException(error, errorMsg));
		});
	}

	public Uni<NGSILDOperationResult> updateRegistration(String tenant, String registrationId,
			Map<String, Object> entry) {
		AppendCSourceRequest request = new AppendCSourceRequest(tenant, registrationId, entry);
		return cSourceInfoDAO.updateRegistration(request).onItem().transformToUni(rowset -> {
			if (rowset.rowCount() > 0) {
				// no need to query regs again they are not distributed
				// request.setPayload(rowset.iterator().next().getJsonObject(0).getMap());
				MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, emitter, objectMapper);
				NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.OPERATION_UPDATE_REGISTRATION,
						registrationId);
				result.addSuccess(new CRUDSuccess(null, null, request.getId(), Sets.newHashSet()));
				return Uni.createFrom().item(result);
			} else {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "Registration not found"));
			}
		}).onFailure().recoverWithUni(
				// TODO do some proper error handling depending on the sql code
				e -> {
					if (e instanceof ResponseException) {
						return Uni.createFrom().failure((ResponseException) e);
					} else {
						return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError, e.getMessage()));
					}
				});
	}

	public Uni<Map<String, Object>> retrieveRegistration(String tenant, String registrationId) {
		return cSourceInfoDAO.getRegistrationById(tenant, registrationId).onItem().transformToUni(rowSet -> {
			if (rowSet.size() == 0) {
				return Uni.createFrom()
						.failure(new ResponseException(ErrorType.NotFound, registrationId + "was not found"));
			}
			JsonObject first = rowSet.iterator().next().getJsonObject(0);
			if (first == null) {
				return Uni.createFrom()
						.failure(new ResponseException(ErrorType.NotFound, registrationId + "was not found"));
			}
			return Uni.createFrom().item(first.getMap());
		});

	}

	public Uni<NGSILDOperationResult> deleteRegistration(String tenant, String registrationId) {
		DeleteCSourceRequest request = new DeleteCSourceRequest(tenant, registrationId);
		return cSourceInfoDAO.deleteRegistration(request).onItem().transformToUni(rowset -> {
			if (rowset.rowCount() > 0) {
				// add the deleted entry
				request.setPayload(rowset.iterator().next().getJsonObject(0).getMap());
				MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, emitter, objectMapper);
				NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.OPERATION_DELETE_REGISTRATION,
						registrationId);
				result.addSuccess(new CRUDSuccess(null, null, request.getId(), Sets.newHashSet()));
				return Uni.createFrom().item(result);

			} else {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "Registration not found"));
			}
		}).onFailure().recoverWithUni(
				// TODO do some proper error handling depending on the sql code
				e -> {
					if (e instanceof ResponseException) {
						return Uni.createFrom().failure((ResponseException) e);
					} else {
						return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError, e.getMessage()));
					}
				});
	}

	public Uni<QueryResult> queryRegistrations(String tenant, Set<String> ids, TypeQueryTerm typeQuery,
											   String idPattern, AttrsQueryTerm attrsQuery, CSFQueryTerm csf, GeoQueryTerm geoQuery,
											   ScopeQueryTerm scopeQuery, QQueryTerm qQueryTerm, int limit, int offset, boolean count) {
		return cSourceInfoDAO
				.query(tenant, ids, typeQuery, idPattern, attrsQuery, csf, geoQuery, scopeQuery,qQueryTerm, limit, offset, count)
				.onItem().transform(rows -> {
					QueryResult result = new QueryResult();
					if(rows.size()==0){
						result.setData(new ArrayList<>());
						return result;
					}
					long countLong = rows.iterator().next().getLong(1);
					if (count) {
						result.setCount(countLong);
					}
					RowIterator<Row> it = rows.iterator();
					Row next;
					List<Map<String, Object>> resultData = new ArrayList<>(rows.size());
					while (it.hasNext()) {
						next = it.next();
						resultData.add(next.getJsonObject(0).getMap());
					}
					long leftAfter = countLong - (offset + limit);
					leftAfter = (leftAfter < 0) ? 0 : leftAfter;
                    result.setResultsLeftAfter(leftAfter);
					result.setResultsLeftBefore((long) offset);
					result.setLimit(limit);
					result.setOffset(offset);
					result.setData(resultData);
					return result;
				});

	}

	@IfBuildProperty(name = "scorpio.fedupdate", stringValue = "active", enableIfMissing = false)
	@Scheduled(every = "${scorpio.fedupdaterate}", delayed = "${scorpio.startupdelay}")
	Uni<Void> checkInternalAndSendUpdateIfNeeded() {
//		return cSourceInfoDAO.getAllTenants().onItem().transformToUni(tenants -> {
//			List<Uni<Map<String, Object>>> unis = Lists.newArrayList();
//			tenants.forEach(tenant -> {
//				unis.add(retrieveRegistration(tenant, AUTO_REG_MODE));
//			});
//			return Uni.combine().all().unis(unis).combinedWith(list -> {
//				List<Uni<Void>> unisForCall = Lists.newArrayList();
//				for (String fedBroker : FED_BROKERS) {
//					String finalFedBroker;
//					if (!fedBroker.endsWith("/")) {
//						finalFedBroker = fedBroker + "/";
//					} else {
//						finalFedBroker = fedBroker;
//					}
//					list.forEach(obj -> {
//						HashMap<String, Object> copyToSend = (HashMap<String, Object>) obj;
//						String csourceId = microServiceUtils.getGatewayURL().toString();
//						copyToSend.put(NGSIConstants.JSON_LD_ID, csourceId);
//						String body = JsonUtils
//								.toPrettyString(JsonLdProcessor.compact(copyToSend, null, HttpUtils.opts));
//						unisForCall.add(webClient.patchAbs(finalFedBroker + "csourceRegistrations/" + csourceId)
//								.putHeader("Content-Type", "application/json").sendBuffer(Buffer.buffer(body))
//								.onItem().transformToUni(i -> {
//									if (i.statusCode() == HttpResponseStatus.NOT_FOUND.code()) {
//										return webClient.postAbs(finalFedBroker + "csourceRegistrations/")
//												.putHeader("Content-Type", "application/json")
//												.sendBuffer(Buffer.buffer(body)).onItem().transformToUni(r -> {
//													if (r.statusCode() >= 200 && r.statusCode() < 300) {
//														return Uni.createFrom().nullItem();
//													}
//													return Uni.createFrom().failure(new ResponseException(
//															ErrorType.InternalError, r.bodyAsString()));
//												});
//									}
//									return Uni.createFrom().voidItem();
//								}).onFailure().retry().atMost(5).onFailure().recoverWithUni(e -> {
//									logger.error("Failed to register with fed broker", e);
//									return Uni.createFrom().voidItem();
//								}));
//					});
//
//				}
//				return Uni.combine().all().unis(unis).collectFailures()
//						.combinedWith(l -> Uni.createFrom().voidItem());
//			});
//		});
		Object[] brokersNames = fedMap.keySet().toArray();
		List<Uni<Void>> unis = new ArrayList<>();
		for (Object brokerName : brokersNames) {
			Map<String, String> brokerDetails = fedMap.get(brokerName.toString());
			String sourceTenant = brokerDetails.get("sourcetenant");
			String targetTenant = brokerDetails.get("targettenant");
			String regType = brokerDetails.get("regtype");
			String url = brokerDetails.get("url");
			String finalUrl = url.endsWith("/") ? url : url + "/";
			unis.add(cSourceInfoDAO.isTenantPresent(sourceTenant).onItem().transformToUni(present -> {
				if (present) {
					return retrieveRegistration(sourceTenant, regType).onItem().transformToUni(body -> {
						String csourceId = microServiceUtils.getGatewayURL().toString();
						body.put("@id", csourceId);
						return ldService.compact(body, null, HttpUtils.opts).onItem().transformToUni(compacted -> {
							String compact;
							try {
								compact = JsonUtils.toPrettyString(compacted);
							} catch (Exception e) {
								return Uni.createFrom().failure(new Throwable("Unable to compact"));
							}
							return webClient.patchAbs(finalUrl + "csourceRegistrations/" + csourceId)
									.putHeader("Content-Type", "application/json")
									.putHeader("NGSILD-Tenant", targetTenant).sendBuffer(Buffer.buffer(compact))
									.onItem().transformToUni(i -> {
										if (i.statusCode() == HttpResponseStatus.NOT_FOUND.code()) {
											return webClient.post(finalUrl + "csourceRegistrations/")
													.putHeader("Content-Type", "application/json")
													.putHeader("NGSILD-Tenant", targetTenant)
													.sendBuffer(Buffer.buffer(compact)).onItem().transformToUni(r -> {
														if (r.statusCode() >= 200 && r.statusCode() < 300) {
															return Uni.createFrom().nullItem();
														}
														return Uni.createFrom().failure(new ResponseException(
																ErrorType.InternalError, r.bodyAsString()));
													});
										}
										return Uni.createFrom().voidItem();
									}).onFailure().retry().atMost(5).onFailure().recoverWithUni(e -> {
										logger.error("Failed to register with fed broker " + brokerName, e);
										return Uni.createFrom().voidItem();
									});
						});
					});
				}
				return Uni.createFrom().voidItem();

			}));
		}
		return Uni.combine().all().unis(unis).collectFailures().discardItems();
	}

}
