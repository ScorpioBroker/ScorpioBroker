package eu.neclab.ngsildbroker.registry.subscriptionmanager.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdError;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.DeleteSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.UpdateSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.repository.RegistrySubscriptionInfoDAO;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;

@Singleton
public class RegistrySubscriptionService {

	@Inject
	RegistrySubscriptionInfoDAO regDAO;

	protected Table<String, String, SubscriptionRequest> tenant2subscriptionId2Subscription = HashBasedTable.create();

	public Uni<NGSILDOperationResult> createSubscription(String tenant, Map<String, Object> subscription,
			Context context) {
		SubscriptionRequest request;
		try {
			request = new SubscriptionRequest(tenant, subscription, context);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		return regDAO.createSubscription(request).onItem().transform(t -> {
			tenant2subscriptionId2Subscription.put(tenant, request.getId(), request);
			return new NGSILDOperationResult(AppConstants.CREATE_SUBSCRIPTION_REQUEST, request.getId());
		}).onFailure().recoverWithUni(e -> {
			// TODO sql check
			return Uni.createFrom().failure(new ResponseException(ErrorType.AlreadyExists,
					"Subscription with id " + request.getId() + " exists"));
		});
	}

	public Uni<NGSILDOperationResult> updateSubscription(String tenant, String subscriptionId,
			Map<String, Object> update, Context context) {
		UpdateSubscriptionRequest request = new UpdateSubscriptionRequest(tenant, subscriptionId, update, context);
		return regDAO.updateSubscription(request).onItem().transformToUni(t -> {
			if (t.rowCount() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "subscription not found"));
			}
			Row row = t.iterator().next();
			SubscriptionRequest updatedRequest;
			try {
				updatedRequest = new SubscriptionRequest(tenant, row.getJsonObject(0).getMap(),
						new Context().parse(row.getJsonObject(0).getMap(), false));
			} catch (Exception e) {
				return Uni.createFrom().failure(e);
			}
			tenant2subscriptionId2Subscription.put(tenant, request.getId(), updatedRequest);
			return Uni.createFrom()
					.item(new NGSILDOperationResult(AppConstants.UPDATE_SUBSCRIPTION_REQUEST, subscriptionId));
		});
	}

	public Uni<NGSILDOperationResult> deleteSubscription(String tenant, String subscriptionId) {
		DeleteSubscriptionRequest request = new DeleteSubscriptionRequest(tenant, subscriptionId);
		return regDAO.deleteSubscription(request).onItem().transform(t -> {
			tenant2subscriptionId2Subscription.remove(tenant, subscriptionId);
			return new NGSILDOperationResult(AppConstants.DELETE_SUBSCRIPTION_REQUEST, subscriptionId);
		});
	}

	public Uni<QueryResult> getAllSubscriptions(String tenant, int limit, int offset) {
		return regDAO.getAllSubscriptions(tenant, limit, offset).onItem().transform(rows -> {
			QueryResult result = new QueryResult();
			Row next = null;
			RowIterator<Row> it = rows.iterator();
			List<Map<String, Object>> resultData = new ArrayList<Map<String, Object>>(rows.size());
			while (it.hasNext()) {
				next = it.next();
				resultData.add(next.getJsonObject(1).getMap());
			}
			result.setData(resultData);
			if (next == null) {
				return result;
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
			return result;
		});
	}

	public Uni<Map<String, Object>> getSubscription(String tenant, String subscriptionId) {
		return regDAO.getSubscription(tenant, subscriptionId).onItem().transformToUni(rows -> {
			if (rows.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "subscription not found"));
			}
			return Uni.createFrom().item(rows.iterator().next().getJsonObject(0).getMap());
		});
	}

	public Uni<Void> checkSubscriptions(BaseRequest message) {
		Collection<SubscriptionRequest> potentialSubs = tenant2subscriptionId2Subscription.column(message.getTenant())
				.values();
		List<Uni<Void>> unis = Lists.newArrayList();
		for (SubscriptionRequest potentialSub : potentialSubs) {
			switch (message.getRequestType()) {
			case AppConstants.UPDATE_REQUEST:
				if (shouldFire(message.getPayload(), potentialSub)) {
					unis.add(regDAO.getRegById(message.getTenant(), message.getId()).onItem().transformToUni(rows -> {
						return sendNotification(potentialSub, rows.iterator().next().getJsonObject(0).getMap());
					}));
				}
				break;
			case AppConstants.CREATE_REQUEST:
			case AppConstants.DELETE_REQUEST:
				unis.add(sendNotification(potentialSub, message.getPayload()));
			default:
				break;
			}

		}
		return Uni.combine().all().unis(unis).combinedWith(null);
	}

	private Uni<Void> sendNotification(SubscriptionRequest potentialSub, Map<String, Object> reg) {
		if(shouldSendOut(potentialSub, reg)) {
			
		}
		return Uni.createFrom().voidItem();
	}

	private boolean shouldSendOut(SubscriptionRequest potentialSub, Map<String, Object> reg) {
		// TODO Auto-generated method stub
		return false;
	}

	public Uni<Void> handleInternalSubscription(SubscriptionRequest message) {
		// TODO Auto-generated method stub
		return null;
	}

	protected boolean shouldFire(Map<String, Object> entry, SubscriptionRequest subscription) {
		List<String> attribs = subscription.getSubscription().getAttributeNames();
		if (attribs == null || attribs.isEmpty()) {
			return true;
		}
		List<Map<String, Object>> information = (List<Map<String, Object>>) entry
				.get(NGSIConstants.NGSI_LD_INFORMATION);
		for (Map<String, Object> informationEntry : information) {
			Object propertyNames = informationEntry.get(NGSIConstants.NGSI_LD_PROPERTIES);
			Object relationshipNames = informationEntry.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
			if (relationshipNames == null && relationshipNames == null) {
				return true;
			}
			if (relationshipNames != null) {
				List<Map<String, String>> list = (List<Map<String, String>>) relationshipNames;
				for (Map<String, String> relationshipEntry : list) {
					if (attribs.contains(relationshipEntry.get(NGSIConstants.JSON_LD_ID))) {
						return true;
					}
				}
			}
			if (propertyNames != null) {
				List<Map<String, String>> list = (List<Map<String, String>>) propertyNames;
				for (Map<String, String> propertyEntry : list) {
					if (attribs.contains(propertyEntry.get(NGSIConstants.JSON_LD_ID))) {
						return true;
					}
				}
			}
		}
		return false;
	}

}
