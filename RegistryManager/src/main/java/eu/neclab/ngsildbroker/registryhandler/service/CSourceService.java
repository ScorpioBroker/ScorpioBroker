package eu.neclab.ngsildbroker.registryhandler.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceRequest;
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
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.registryhandler.controller.RegistryController;
import eu.neclab.ngsildbroker.registryhandler.repository.CSourceDAO;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;

@Singleton
public class CSourceService {

	private final static Logger logger = LoggerFactory.getLogger(RegistryController.class);

	@Inject
	MicroServiceUtils microServiceUtils;

	@Inject
	CSourceDAO cSourceInfoDAO;

	@Inject
	@Channel(AppConstants.REGISTRY_CHANNEL)
	@Broadcast
	MutinyEmitter<BaseRequest> kafkaSenderInterface;

	@ConfigProperty(name = "scorpio.federation.registrationtype", defaultValue = "types")
	String AUTO_REG_MODE;

	@ConfigProperty(name = "scorpio.federation.hosts", defaultValue = "")
	String FED_BROKERS_CONFIG;

	String[] FED_BROKERS;

	@ConfigProperty(name = "scorpio.topics.registry")
	String CSOURCE_TOPIC;

	@ConfigProperty(name = "scorpio.directDB", defaultValue = "true")
	boolean directDB;

	@Inject
	Vertx vertx;

	Map<String, Object> myRegistryInformation;

	private WebClient webClient;

	@PostConstruct
	void setup() {
		this.webClient = new WebClient(vertx);
		if (FED_BROKERS_CONFIG.isBlank()) {
			FED_BROKERS = new String[0];
		} else {
			FED_BROKERS = FED_BROKERS_CONFIG.split(",");
		}
	}

	public Uni<NGSILDOperationResult> createRegistration(String tenant, Map<String, Object> registration) {
		CreateCSourceRequest request = new CreateCSourceRequest(tenant, registration);
		return cSourceInfoDAO.createRegistration(request).onItem().transformToUni(rowset -> {
			return kafkaSenderInterface.send(request).onItem().transform(v -> {
				NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.OPERATION_CREATE_REGISTRATION,
						(String) registration.get(NGSIConstants.JSON_LD_ID));
				result.addSuccess(new CRUDSuccess(null, null));
				return result;
			});
		}).onFailure().recoverWithUni(
				// TODO do some proper error handling depending on the sql code
				e -> Uni.createFrom()
						.failure(new ResponseException(ErrorType.AlreadyExists, "Registration already exists")));
	}

	public Uni<NGSILDOperationResult> updateRegistration(String tenant, String registrationId,
			Map<String, Object> entry) {
		AppendCSourceRequest request = new AppendCSourceRequest(tenant, registrationId, entry);
		return cSourceInfoDAO.updateRegistration(request).onItem().transformToUni(rowset -> {
			if (rowset.rowCount() > 0) {
				// no need to query regs again they are not distributed 
				// request.setPayload(rowset.iterator().next().getJsonObject(0).getMap());
				return kafkaSenderInterface.send(request).onItem().transform(v -> {
					NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.OPERATION_UPDATE_REGISTRATION,
							registrationId);
					result.addSuccess(new CRUDSuccess(null, null));
					return result;
				});
			} else {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "Registration not found"));
			}
		}).onFailure().recoverWithUni(
				// TODO do some proper error handling depending on the sql code
				e -> Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "Registration not found")));
	}

	public Uni<Map<String, Object>> retrieveRegistration(String tenant, String registrationId) {
		return cSourceInfoDAO.getRegistrationById(tenant, registrationId).onItem().transformToUni(rowSet -> {
			if (rowSet.size() == 0) {
				return Uni.createFrom()
						.failure(new ResponseException(ErrorType.NotFound, registrationId + "was not found"));
			}
			return Uni.createFrom().item(rowSet.iterator().next().getJsonObject(0).getMap());
		});

	}

	public Uni<NGSILDOperationResult> deleteRegistration(String tenant, String registrationId) {
		DeleteCSourceRequest request = new DeleteCSourceRequest(tenant, registrationId);
		return cSourceInfoDAO.deleteRegistration(request).onItem().transformToUni(rowset -> {
			if (rowset.rowCount() > 0) {
				// add the deleted entry
				request.setPayload(rowset.iterator().next().getJsonObject(0).getMap());
				return kafkaSenderInterface.send(request).onItem().transform(v -> {
					NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.OPERATION_DELETE_REGISTRATION,
							registrationId);
					result.addSuccess(new CRUDSuccess(null, null));
					return result;
				});
			} else {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "Registration not found"));
			}
		}).onFailure().recoverWithUni(
				// TODO do some proper error handling depending on the sql code
				e -> Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "Registration not found")));
	}

	public Uni<QueryResult> queryRegistrations(String tenant, Set<String> ids, TypeQueryTerm typeQuery,
			String idPattern, AttrsQueryTerm attrsQuery, CSFQueryTerm csf, GeoQueryTerm geoQuery,
			ScopeQueryTerm scopeQuery, int limit, int offset, boolean count) {
		return cSourceInfoDAO
				.query(tenant, ids, typeQuery, idPattern, attrsQuery, csf, geoQuery, scopeQuery, limit, offset, count)
				.onItem().transform(rows -> {
					QueryResult result = new QueryResult();
					if (limit == 0 && count) {
						result.setCount(rows.iterator().next().getLong(0));
					} else {
						RowIterator<Row> it = rows.iterator();
						Row next = null;
						List<Map<String, Object>> resultData = new ArrayList<Map<String, Object>>(rows.size());
						while (it.hasNext()) {
							next = it.next();
							resultData.add(next.getJsonObject(1).getMap());
						}
						Long resultCount = next.getLong(0);
						result.setCount(resultCount);
						long leftAfter = resultCount - (offset + limit);
						if (leftAfter < 0) {
							leftAfter = 0;
						}
						long leftBefore = offset;
						result.setResultsLeftAfter(leftAfter);
						result.setResultsLeftBefore(leftBefore);
						result.setLimit(limit);
						result.setOffset(offset);
					}
					return result;
				});

	}

	@IfBuildProperty(name = "scorpio.fedupdate", stringValue = "active", enableIfMissing = false)
	@Scheduled(every = "{scorpio.fedupdaterate}")
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

		return null;
	}

}
