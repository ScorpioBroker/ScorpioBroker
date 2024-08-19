package eu.neclab.ngsildbroker.commons.datatypes.requests.subscription;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.github.jsonldjava.core.Context;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityCache;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.DataSetIdTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.OmitTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.PickTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class SubscriptionRequest implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6085042631683979741L;

	protected Subscription subscription;

	protected Context context;

	protected String tenant;

	protected String id;

	protected Map<String, Object> payload;

	protected int requestType;

	protected long sendTimestamp;

	private String contextId;

	public SubscriptionRequest() {
		// default constructor for serialization
	}

	public SubscriptionRequest(String tenant, Map<String, Object> subscription, Context context)
			throws ResponseException {
		this(tenant, (String) subscription.get(NGSIConstants.JSON_LD_ID), subscription, context, false);
	}

	public SubscriptionRequest(String tenant, String id, Map<String, Object> subscription, Context context,
			boolean update) throws ResponseException {
		this.tenant = tenant;
		this.id = id;
		this.payload = subscription;
		this.requestType = AppConstants.CREATE_SUBSCRIPTION_REQUEST;
		this.context = context;
		this.subscription = Subscription.expandSubscription(subscription, context, update);

	}

	protected SubscriptionRequest(String tenant, String subscriptionId, int requestType) {
		this.tenant = tenant;
		this.id = subscriptionId;
		this.requestType = requestType;
	}

	public void setPayload(Map<String, Object> payload) {
		this.payload = payload;
		try {
			if (payload != null)
				this.subscription = Subscription.expandSubscription(payload, context, false);
		} catch (ResponseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@JsonSetter("payload")
	void setPayloadForDeserialize(Map<String, Object> payload) {
		this.payload = payload;
	}

	public Subscription getSubscription() {
		return subscription;
	}

	public void setSubscription(Subscription subscription) {
		this.subscription = subscription;
	}

	public Context getContext() {
		return context;
	}

	public void setContext(Context context) {
		this.context = context;
	}

	@Override
	public String toString() {
		return "SubscriptionRequest [subscription=" + subscription + ", context=" + context + "]";
	}

	public String getTenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getRequestType() {
		return requestType;
	}

	public void setRequestType(int requestType) {
		this.requestType = requestType;
	}

	public Map<String, Object> getPayload() {
		return payload;
	}

	public long getSendTimestamp() {
		return sendTimestamp;
	}

	public void setSendTimestamp(long sendTimestamp) {
		this.sendTimestamp = sendTimestamp;
	}

	public boolean firstCheckToSendOut(String entityId, Map<String, Object> payload, String allTypesSub) {
		Subscription sub = getSubscription();
		if (!sub.getIsActive() || sub.getExpiresAt() < System.currentTimeMillis()) {
			return false;
		}

		if (sub.isLocalOnly() && sub.getEntities() == null) {
			return true;
		}

		if (sub.getAttributeNames() != null && !sub.getAttributeNames().isEmpty()
				&& Sets.intersection(sub.getAttributeNames(), payload.keySet()).isEmpty()) {
			return false;
		}

		for (EntityInfo entityInfo : sub.getEntities()) {
			if (entityInfo.getTypeTerm().getAllTypes().contains(allTypesSub)
					|| (entityInfo.getId() != null && entityInfo.getId().toString().equals(entityId))
					|| (entityInfo.getIdPattern() != null && entityId.matches(entityInfo.getIdPattern()))
					|| (entityInfo.getIdPattern() == null && entityInfo.getId() == null)) {
				return true;
			}
		}

		return false;
	}

	public boolean fullEntityCheckToSendOut(String entityId, Map<String, Object> payload, String allTypesSub,
			Set<String> jsonKeys) {
		Subscription sub = getSubscription();
		boolean typeQueryResult = false;
		for (EntityInfo entityInfo : sub.getEntities()) {
			TypeQueryTerm typeTerm = entityInfo.getTypeTerm();
			if (typeTerm != null) {
				if (typeTerm.getAllTypes().contains(allTypesSub)) {
					typeQueryResult = true;
					break;
				}
				typeQueryResult = typeTerm.calculate((List<String>) payload.get(NGSIConstants.JSON_LD_TYPE));
				if (typeQueryResult) {
					if (entityInfo.getId() != null) {
						if (entityInfo.getId().toString().equals(entityId)) {
							break;
						} else {
							typeQueryResult = false;
						}
					} else if (entityInfo.getIdPattern() != null) {
						if (entityId.matches(entityInfo.getIdPattern())) {
							break;
						} else {
							typeQueryResult = false;
						}
					} else {
						break;
					}

				}
			}
		}
		if (!typeQueryResult) {
			return false;
		}
		if (sub.getNotification().getAttrs() != null && !sub.getNotification().getAttrs().calculateEntity(payload)) {
			return false;
		}
		boolean qResult = (sub.getLdQuery() != null
				&& !sub.getLdQuery().calculateEntity(payload, new EntityCache(), jsonKeys, false));
		boolean scopeResult = (sub.getScopeQuery() != null && !sub.getScopeQuery().calculateEntity(payload));
		boolean geoQResult = (sub.getLdGeoQuery() != null && !sub.getLdGeoQuery().calculateEntity(payload));

		boolean pickResult = (sub.getNotification().getPick() != null
				&& !sub.getNotification().getPick().calculateEntity(payload, false, null, null, false));
		boolean omitResult = (sub.getNotification().getOmit() != null
				&& !sub.getNotification().getOmit().calculateEntity(payload, false, null, null, false));

		boolean datasetIdResult = (sub.getDatasetIdTerm() != null && !sub.getDatasetIdTerm().calculateEntity(payload));
		if (qResult || scopeResult || geoQResult || pickResult || omitResult || datasetIdResult) {
			return false;
		}

		return true;
	}

	public boolean doJoin() {
		return subscription.getNotification().getJoin() != null || subscription.getNotification().getJoinLevel() > 0;
	}

	public Map<String, Object> getAsQueryBody(Set<String> idsTbu, String atContextUrl) {
		Map<String, Object> result = Maps.newHashMap();
		result.put(NGSIConstants.TYPE, NGSIConstants.QUERY_TYPE);

		List<EntityInfo> entities = subscription.getEntities();
		if (entities != null && !entities.isEmpty()) {
			List<Map<String, String>> entitiesEntry = new ArrayList<>(entities.size());
			for (EntityInfo entityInfo : entities) {
				Map<String, String> entityEntry = Maps.newHashMap();

				TypeQueryTerm typeQuery = entityInfo.getTypeTerm();
				if (typeQuery != null) {
					StringBuilder typeQueryBuilder = new StringBuilder();
					typeQuery.toRequestString(typeQueryBuilder, context);
					entityEntry.put(NGSIConstants.TYPE, typeQueryBuilder.toString());
				}
				if (idsTbu == null) {
					if (entityInfo.getId() != null) {
						entityEntry.put(NGSIConstants.ID, entityInfo.getId().toString());
					} else if (entityInfo.getIdPattern() != null) {
						entityEntry.put(NGSIConstants.QUERY_PARAMETER_IDPATTERN, entityInfo.getIdPattern());
					}
				} else {
					entityEntry.put(NGSIConstants.ID, StringUtils.join(idsTbu, ','));
				}
				entitiesEntry.add(entityEntry);
			}
			result.put(NGSIConstants.NGSI_LD_ENTITIES_SHORT, entitiesEntry);
		}

		GeoQueryTerm geoQ = subscription.getLdGeoQuery();
		if (geoQ != null) {
			geoQ.addToRequestParams(result, geoQ.getShape(), geoQ.getGeorel());
		}
		ScopeQueryTerm scopeQ = subscription.getScopeQuery();
		if (scopeQ != null) {
			result.put(NGSIConstants.QUERY_PARAMETER_SCOPE_QUERY, scopeQ.getScopeQueryString());
		}
		QQueryTerm qQuery = subscription.getLdQuery();
		if (qQuery != null) {
			result.put(NGSIConstants.QUERY_PARAMETER_QUERY, qQuery.toQueryParam(context));
		}
		PickTerm pick = subscription.getNotification().getPick();
		if (pick != null) {
			result.put(NGSIConstants.QUERY_PARAMETER_PICK, pick.toQueryParam(context));
		}
		OmitTerm omit = subscription.getNotification().getOmit();
		if (omit != null) {
			result.put(NGSIConstants.QUERY_PARAMETER_OMIT, omit.toQueryParam(context));
		}
		AttrsQueryTerm attrs = subscription.getNotification().getAttrs();
		if (attrs != null) {
			result.put(NGSIConstants.QUERY_PARAMETER_ATTRS, attrs.toQueryBodyEntry(context));
		}
		DataSetIdTerm datasetId = subscription.getDatasetIdTerm();
		if (datasetId != null) {

		}
		String join = subscription.getNotification().getJoin();
		int joinLevel = subscription.getNotification().getJoinLevel();
		if (join != null && joinLevel > 0) {
			result.put(NGSIConstants.QUERY_PARAMETER_JOIN, join);
			result.put(NGSIConstants.QUERY_PARAMETER_JOINLEVEL, joinLevel + "");
		}
		result.put(NGSIConstants.JSON_LD_CONTEXT, atContextUrl + "/" + NGSIConstants.JSONLD_CONTEXTS + contextId);
		return result;
	}

	public String getContextId() {
		return contextId;
	}

	public void setContextId(String contextId) {
		this.contextId = contextId;
	}

}
