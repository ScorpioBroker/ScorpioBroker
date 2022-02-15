package eu.neclab.ngsildbroker.commons.subscriptionbase;

import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_CONTAINS;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_DISJOINT;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_EQUALS;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_INTERSECTS;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_NEAR;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_OVERLAPS;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_WITHIN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.locationtech.spatial4j.SpatialPredicate;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory.LineStringBuilder;
import org.locationtech.spatial4j.shape.ShapeFactory.PolygonBuilder;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.filosganga.geogson.model.LineString;
import com.github.filosganga.geogson.model.Point;
import com.github.filosganga.geogson.model.Polygon;
import com.github.filosganga.geogson.model.positions.SinglePosition;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.GeoProperty;
import eu.neclab.ngsildbroker.commons.datatypes.GeoPropertyEntry;
import eu.neclab.ngsildbroker.commons.datatypes.LDGeoQuery;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.NotificationHandler;
import eu.neclab.ngsildbroker.commons.interfaces.SubscriptionCRUDService;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;

public abstract class BaseSubscriptionService implements SubscriptionCRUDService {

	protected final static Logger logger = LoggerFactory.getLogger(BaseSubscriptionService.class);

	private final String ALL_TYPES_TYPE = "()";

	private NotificationHandlerREST notificationHandlerREST;
	private IntervalNotificationHandler intervalHandlerREST;

	//private NotificationHandlerMQTT notificationHandlerMQTT;
	private IntervalNotificationHandler intervalHandlerMQTT;

	private Timer watchDog = new Timer(true);

	// protected WebClient webClient = BeanTools.getWebClient();

	@Inject
	Vertx vertx;

	WebClient webClient;

	private SubscriptionInfoDAOInterface subscriptionInfoDAO;

	private boolean sendInitialNotification;
	private boolean sendDeleteNotification;

	private JtsShapeFactory shapeFactory = JtsSpatialContext.GEO.getShapeFactory();

	protected Table<String, String, SubscriptionRequest> tenant2subscriptionId2Subscription = HashBasedTable.create();
	private Table<String, String, TimerTask> subId2TimerTask = HashBasedTable.create();
	private Table<String, String, List<SubscriptionRequest>> type2EntitiesSubscriptions = HashBasedTable.create();
	private HashMap<SubscriptionRequest, Long> sub2CreationTime = new HashMap<SubscriptionRequest, Long>();
	private Table<String, String, List<Object>> tenantId2subscriptionId2Context = HashBasedTable.create();

	private Table<String, String, Set<String>> tenant2Ids2Type;

	@PostConstruct
	void setup() {
		subscriptionInfoDAO = getSubscriptionInfoDao();
		webClient = WebClient.create(vertx);
		try {
			this.tenant2Ids2Type = subscriptionInfoDAO.getIds2Type();
		} catch (ResponseException e) {
			logger.error(e.getLocalizedMessage());
		}
		sendInitialNotification = sendInitialNotification();
		sendDeleteNotification = sendDeleteNotification();
		notificationHandlerREST = new NotificationHandlerREST(webClient);
		Subscription temp = new Subscription();
		temp.setId("invalid:base");
		intervalHandlerREST = new IntervalNotificationHandler(notificationHandlerREST, subscriptionInfoDAO,
				getNotification(new SubscriptionRequest(temp, null, null, AppConstants.UPDATE_REQUEST), null,
						AppConstants.UPDATE_REQUEST));

		//notificationHandlerMQTT = new NotificationHandlerMQTT();
		intervalHandlerMQTT = new IntervalNotificationHandler(null, subscriptionInfoDAO,
				getNotification(new SubscriptionRequest(temp, null, null, AppConstants.UPDATE_REQUEST), null,
						AppConstants.UPDATE_REQUEST));
		logger.trace("call loadStoredSubscriptions() ::");
		loadStoredSubscriptions();

	}

	protected abstract boolean sendDeleteNotification();

	protected abstract boolean sendInitialNotification();

	protected abstract SubscriptionInfoDAOInterface getSubscriptionInfoDao();

	@PreDestroy
	protected void deconstructor() {
		synchronized (tenant2subscriptionId2Subscription) {
			subscriptionInfoDAO.storedSubscriptions(tenant2subscriptionId2Subscription);
		}
	}

	private void loadStoredSubscriptions() {
		synchronized (this.tenant2subscriptionId2Subscription) {
			List<String> subscriptions = subscriptionInfoDAO.getStoredSubscriptions();
			for (String subscriptionString : subscriptions) {
				try {
					subscribe(DataSerializer.getSubscriptionRequest(subscriptionString));
				} catch (ResponseException e) {
					logger.error("Failed to load stored subscription", e);
				}
			}
		}
	}

	public String subscribe(SubscriptionRequest subscriptionRequest) throws ResponseException {
		logger.debug("Subscribe got called " + subscriptionRequest.getSubscription().toString());
		Subscription subscription = subscriptionRequest.getSubscription();
		if (subscription.getId() == null) {
			subscription.setId(generateUniqueSubId(subscription));
		} else {
			synchronized (tenant2subscriptionId2Subscription) {
				if (this.tenant2subscriptionId2Subscription.contains(subscriptionRequest.getTenant(),
						subscription.getId().toString())) {
					throw new ResponseException(ErrorType.AlreadyExists,
							subscription.getId().toString() + " already exists");

				}
			}
		}
		synchronized (tenant2subscriptionId2Subscription) {
			this.tenant2subscriptionId2Subscription.put(subscriptionRequest.getTenant(),
					subscription.getId().toString(), subscriptionRequest);
		}

		String endpointProtocol = subscription.getNotification().getEndPoint().getUri().getScheme();
		if (subscription.getTimeInterval() > 0) {
			if (endpointProtocol.equals("mqtt")) {
				intervalHandlerMQTT.addSub(subscriptionRequest);
			} else {
				intervalHandlerREST.addSub(subscriptionRequest);
			}
		} else {
			this.tenantId2subscriptionId2Context.put(subscriptionRequest.getTenant(), subscription.getId().toString(),
					subscriptionRequest.getContext());
			this.sub2CreationTime.put(subscriptionRequest, System.currentTimeMillis());
			List<EntityInfo> entities = subscription.getEntities();
			if (entities == null || entities.isEmpty()) {
				putInTable(this.type2EntitiesSubscriptions, subscriptionRequest.getTenant(), ALL_TYPES_TYPE,
						subscriptionRequest);
			} else {
				for (EntityInfo info : subscription.getEntities()) {
					putInTable(this.type2EntitiesSubscriptions, subscriptionRequest.getTenant(), info.getType(),
							subscriptionRequest);

				}

			}
			if (subscription.getExpiresAt() != null && subscription.getExpiresAt() > 0) {
				TimerTask cancel = new CancelTask(subscriptionRequest);
				subId2TimerTask.put(subscriptionRequest.getTenant(), subscription.getId().toString(), cancel);
				watchDog.schedule(cancel, subscription.getExpiresAt() - System.currentTimeMillis());
			}
			new Thread() {
				@SuppressWarnings("unchecked")
				public void run() {
					if (sendInitialNotification) {
						try {
							List<String> temp = subscriptionInfoDAO.getEntriesFromSub(subscriptionRequest);
							if (!temp.isEmpty()) {
								List<Map<String, Object>> notifcation = new ArrayList<Map<String, Object>>();
								for (String entry : temp) {
									notifcation.add((Map<String, Object>) JsonUtils.fromString(entry));
								}
								sendNotification(notifcation, subscriptionRequest, AppConstants.CREATE_REQUEST);
							}
						} catch (ResponseException | IOException e) {
							logger.error("Failed to send initial notifcation", e);
						}
					}
				};
			}.start();
		}
		return subscription.getId();
	}

	private void putInTable(Table<String, String, List<SubscriptionRequest>> table, String row, String colum,
			SubscriptionRequest subscriptionRequest) {
		List<SubscriptionRequest> subs = table.get(row, colum);
		if (subs == null) {
			subs = new ArrayList<SubscriptionRequest>();
			table.put(row, colum, subs);
		}
		subs.add(subscriptionRequest);
	}

	private void removeFromTable(Table<String, String, List<SubscriptionRequest>> table, String row, String colum,
			SubscriptionRequest subscriptionRequest) {
		List<SubscriptionRequest> subs = table.get(row, colum);
		if (subs == null) {
			return;
		}
		subs.remove(subscriptionRequest);
		if (subs.isEmpty()) {
			table.remove(row, colum);
		}
	}

	protected abstract String generateUniqueSubId(Subscription subscription);

	public void unsubscribe(String id, ArrayListMultimap<String, String> headers) throws ResponseException {
		String tenant = HttpUtils.getInternalTenant(headers);
		SubscriptionRequest removedSub;
		synchronized (tenant2subscriptionId2Subscription) {
			if (!this.tenant2subscriptionId2Subscription.contains(tenant, id)) {
				throw new ResponseException(ErrorType.NotFound, id + " not found");
			}
			removedSub = this.tenant2subscriptionId2Subscription.get(tenant, id);
			if (removedSub == null) {
				throw new ResponseException(ErrorType.NotFound, id + " not found");
			}

			removedSub = this.tenant2subscriptionId2Subscription.remove(tenant, id);
		}

		synchronized (tenantId2subscriptionId2Context) {
			this.tenantId2subscriptionId2Context.remove(tenant, id);
		}
		intervalHandlerREST.removeSub(id);
		intervalHandlerMQTT.removeSub(id);
		List<EntityInfo> entities = removedSub.getSubscription().getEntities();
		if (entities == null || entities.isEmpty()) {
			synchronized (type2EntitiesSubscriptions) {
				removeFromTable(type2EntitiesSubscriptions, tenant, ALL_TYPES_TYPE, removedSub);
			}
		} else {
			for (EntityInfo info : entities) {
				synchronized (type2EntitiesSubscriptions) {
					removeFromTable(type2EntitiesSubscriptions, tenant, info.getType(), removedSub);
				}
			}
		}
		TimerTask task = subId2TimerTask.get(tenant, id);
		if (task != null) {
			task.cancel();
		}
		// TODO remove remote subscription
	}

	public void updateSubscription(SubscriptionRequest subscriptionRequest) throws ResponseException {
		Subscription subscription = subscriptionRequest.getSubscription();
		String tenant = subscriptionRequest.getTenant();
		SubscriptionRequest oldSubRequest;
		synchronized (tenant2subscriptionId2Subscription) {
			oldSubRequest = tenant2subscriptionId2Subscription.get(tenant, subscription.getId().toString());
			if (oldSubRequest == null) {
				throw new ResponseException(ErrorType.NotFound, subscription.getId().toString() + " not found");
			}
			Subscription oldSub = oldSubRequest.getSubscription();
			oldSub.update(subscription);
			if (oldSub.getExpiresAt() != null && oldSub.getExpiresAt() > 0) {
				synchronized (subId2TimerTask) {
					TimerTask task = subId2TimerTask.get(subscriptionRequest.getTenant(), oldSub.getId().toString());
					if (task != null) {
						task.cancel();
					} else {
						task = new CancelTask(subscriptionRequest);
					}
					watchDog.schedule(task, oldSub.getExpiresAt() - System.currentTimeMillis());
				}
			}

			this.tenantId2subscriptionId2Context.put(tenant, oldSub.getId().toString(),
					subscriptionRequest.getContext());
		}

	}

	public List<SubscriptionRequest> getAllSubscriptions(ArrayListMultimap<String, String> headers) {
		String tenantId = HttpUtils.getInternalTenant(headers);
		List<SubscriptionRequest> result;
		synchronized (tenant2subscriptionId2Subscription) {
			result = new ArrayList<SubscriptionRequest>(tenant2subscriptionId2Subscription.row(tenantId).values());
		}
		return result;
	}

	public SubscriptionRequest getSubscription(String subscriptionId, ArrayListMultimap<String, String> headers)
			throws ResponseException {
		SubscriptionRequest sub;
		synchronized (tenant2subscriptionId2Subscription) {
			sub = tenant2subscriptionId2Subscription.get(HttpUtils.getInternalTenant(headers), subscriptionId);
		}
		if (sub == null) {
			throw new ResponseException(ErrorType.NotFound, subscriptionId + " not found");
		}
		return sub;
	}

	public void checkSubscriptionsWithAbsolute(BaseRequest request, long messageTime, int messageType) {
		Set<String> types;
		String id = request.getId();
		String tenantId = request.getTenant();
		if (messageType == AppConstants.CREATE_REQUEST) {
			types = getTypesFromEntry(request);
			synchronized (this.tenant2Ids2Type) {
				this.tenant2Ids2Type.put(tenantId, id, types);
			}
		} else {
			synchronized (this.tenant2Ids2Type) {
				types = this.tenant2Ids2Type.remove(request.getTenant(), request.getId());
			}
			if (!sendDeleteNotification) {
				return;
			}
		}
		ArrayList<SubscriptionRequest> subsToCheck = new ArrayList<SubscriptionRequest>();
		List<SubscriptionRequest> subs = getAllTypeBaseRequests(tenantId, types);
		if (subs != null) {
			for (SubscriptionRequest sub : subs) {

				for (EntityInfo entityInfo : sub.getSubscription().getEntities()) {
					if (entityInfo.getId() == null && entityInfo.getIdPattern() == null) {
						subsToCheck.add(sub);
						break;
					}
					if (entityInfo.getId() != null && entityInfo.getId().toString().equals(id)) {
						subsToCheck.add(sub);
						break;
					}
					if (entityInfo.getIdPattern() != null && id.matches(entityInfo.getIdPattern())) {
						subsToCheck.add(sub);
						break;
					}
				}
			}
		}
		addAllTypeSubscriptions(request.getHeaders(), subsToCheck);
		checkSubscriptions(subsToCheck, request, messageType, messageTime);
	}

	protected abstract Set<String> getTypesFromEntry(BaseRequest createRequest);

	private void addAllTypeSubscriptions(ArrayListMultimap<String, String> entityOpHeaders,
			List<SubscriptionRequest> subsToCheck) {
		List<SubscriptionRequest> subs = this.type2EntitiesSubscriptions.get(ALL_TYPES_TYPE,
				HttpUtils.getInternalTenant(entityOpHeaders));
		if (subs == null) {
			return;
		}
		subsToCheck.addAll(subs);
	}

	private void checkSubscriptions(ArrayList<SubscriptionRequest> subsToCheck, BaseRequest request, int methodType,
			long messageTime) {

		for (SubscriptionRequest subscription : subsToCheck) {
			if (messageTime >= sub2CreationTime.get(subscription)) {
				new Thread() {
					public void run() {
						Map<String, Object> data = null;
						try {
							data = generateDataFromBaseOp(request, subscription, methodType);
							if (data != null) {
								ArrayList<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
								dataList.add(data);
								sendNotification(dataList, subscription, methodType);
							}
						} catch (ResponseException e) {
							logger.error("Failed to handle new data for the subscriptions, cause: " + e.getMessage());
						}
					}

				}.start();
			}

		}

	}

	protected void sendNotification(List<Map<String, Object>> dataList, SubscriptionRequest subscription,
			int triggerReason) {
		String endpointProtocol = subscription.getSubscription().getNotification().getEndPoint().getUri().getScheme();
		NotificationHandler handler = getNotificationHandler(endpointProtocol);
		handler.notify(getNotification(subscription, dataList, triggerReason), subscription);
	}

	protected NotificationHandler getNotificationHandler(String endpointProtocol) {
		if (endpointProtocol.equals("mqtt")) {
			return null; //notificationHandlerMQTT;
		} else {
			return notificationHandlerREST;
		}
	}

	protected abstract Notification getNotification(SubscriptionRequest request, List<Map<String, Object>> dataList,
			int triggerReason);

	protected abstract boolean shouldFire(Map<String, Object> entry, SubscriptionRequest subscription);

	private Map<String, Object> generateDataFromBaseOp(BaseRequest request, SubscriptionRequest subscription,
			int methodType) throws ResponseException {
		Map<String, Object> deltaInfo = request.getRequestPayload();

		if (!shouldFire(deltaInfo, subscription)) {
			return null;
		}
		Map<String, Object> fullEntry = new HashMap<String, Object>(request.getFinalPayload());
		if (!evaluateGeoQuery(subscription.getSubscription().getLdGeoQuery(),
				EntityTools.getLocation(fullEntry, subscription.getSubscription().getLdGeoQuery()))) {
			return null;
		}
		if (evaluateQ()) {
			if (subscription.getSubscription().getLdQuery() != null) {
				if (!subscription.getSubscription().getLdQuery().calculate(EntityTools.getBaseProperties(fullEntry))) {
					return null;
				}
			}
		}
		if (evaluateCSF()) {
			if (subscription.getSubscription().getCsf() != null) {
				if (!subscription.getSubscription().getCsf().calculate(EntityTools.getBaseProperties(fullEntry))) {
					return null;
				}
			}
		}
		if (subscription.getSubscription().getScopeQuery() != null) {
			if (!subscription.getSubscription().getScopeQuery().calculate(EntityTools.getScopes(fullEntry))) {
				return null;
			}
		}
		return EntityTools.clearBaseProps(fullEntry, subscription);
	}

	protected abstract boolean evaluateCSF();

	protected abstract boolean evaluateQ();

	private boolean evaluateGeoQuery(LDGeoQuery geoQuery, GeoProperty location) {
		return evaluateGeoQuery(geoQuery, location, -1);
	}

	private boolean evaluateGeoQuery(LDGeoQuery geoQuery, GeoProperty location, double expandArea) {

		if (geoQuery == null) {
			return true;
		}

		String relation = geoQuery.getGeoRelation().getRelation();
		List<Double> coordinates = geoQuery.getCoordinates();

		if (location == null) {
			return false;
		}
		Iterator<GeoPropertyEntry> it = location.getEntries().values().iterator();
		while (it.hasNext()) {
			GeoPropertyEntry next = it.next();
			if (GEO_REL_EQUALS.equals(relation)) {
				if (next.getGeoValue() instanceof Point) {
					List<Double> geoValueAsList = java.util.Arrays.asList(((Point) next.getGeoValue()).lon(),
							((Point) next.getGeoValue()).lat());

					return geoValueAsList.equals(geoQuery.getCoordinates());
				} else {
					// TODO

					return false;
				}
			} else {

				Shape entityShape;
				if (next.getGeoValue() instanceof Point) {
					entityShape = shapeFactory.pointXY(((Point) next.getGeoValue()).lon(),
							((Point) next.getGeoValue()).lat());
				} else if (next.getGeoValue() instanceof Polygon) {
					PolygonBuilder polygonBuilder = shapeFactory.polygon();
					Iterator<SinglePosition> it2 = ((Polygon) next.getGeoValue()).positions().children().iterator()
							.next().children().iterator();
					while (it2.hasNext()) {
						SinglePosition next2 = it2.next();
						polygonBuilder.pointXY(next2.coordinates().getLon(), next2.coordinates().getLat());
					}
					entityShape = polygonBuilder.build();
				} else if (next.getGeoValue() instanceof LineString) {
					LineStringBuilder lineStringBuilder = shapeFactory.lineString();
					Iterator<SinglePosition> it2 = ((Polygon) next.getGeoValue()).positions().children().iterator()
							.next().children().iterator();
					while (it2.hasNext()) {
						SinglePosition next2 = it2.next();
						lineStringBuilder.pointXY(next2.coordinates().getLon(), next2.coordinates().getLat());
					}
					entityShape = lineStringBuilder.build();
				} else {
					logger.error(
							"Unsupported GeoJson type. Currently Point, Polygon and Linestring are supported but was "
									+ next.getGeoValue().getClass().toString());
					return false;
				}
				Shape queryShape;
				switch (geoQuery.getGeometry()) {
				case Point: {
					queryShape = shapeFactory.pointXY(coordinates.get(0), coordinates.get(1));
					break;
				}
				case Polygon: {
					PolygonBuilder polygonBuilder = shapeFactory.polygon();
					for (int i = 0; i < coordinates.size(); i = i + 2) {
						polygonBuilder.pointXY(coordinates.get(i), coordinates.get(i + 1));
					}

					queryShape = polygonBuilder.build();
					break;
				}
				case LineString: {
					LineStringBuilder lineStringBuilder = shapeFactory.lineString();
					for (int i = 0; i < coordinates.size(); i = i + 2) {
						lineStringBuilder.pointXY(coordinates.get(i), coordinates.get(i + 1));
					}
					queryShape = lineStringBuilder.build();
					break;
				}
				default: {
					return false;
				}
				}
				if (GEO_REL_CONTAINS.equals(relation)) {
					return SpatialPredicate.Contains.evaluate(entityShape, queryShape);
				} else if (GEO_REL_DISJOINT.equals(relation)) {
					return SpatialPredicate.IsDisjointTo.evaluate(entityShape, queryShape);
				} else if (GEO_REL_INTERSECTS.equals(relation)) {
					if (expandArea != -1) {
						queryShape = queryShape.getBuffered(expandArea, queryShape.getContext());
					}
					return SpatialPredicate.Intersects.evaluate(entityShape, queryShape);
				} else if (GEO_REL_NEAR.equals(relation)) {
					if (geoQuery.getGeoRelation().getMaxDistance() != null) {
						Shape bufferedShape = queryShape.getBuffered(
								geoQuery.getGeoRelation().getMaxDistanceAsDouble() * DistanceUtils.KM_TO_DEG,
								queryShape.getContext());
						return SpatialPredicate.IsWithin.evaluate(entityShape, bufferedShape);
					} else if (geoQuery.getGeoRelation().getMinDistance() != null) {
						Shape bufferedShape = queryShape.getBuffered(
								geoQuery.getGeoRelation().getMinDistanceAsDouble() * DistanceUtils.KM_TO_DEG,
								queryShape.getContext());
						return !SpatialPredicate.IsWithin.evaluate(entityShape, bufferedShape);
					} else {
						return false;
					}

				} else if (GEO_REL_OVERLAPS.equals(relation)) {
					return SpatialPredicate.Overlaps.evaluate(entityShape, queryShape);
				} else if (GEO_REL_WITHIN.equals(relation)) {
					if (expandArea != -1) {
						queryShape = queryShape.getBuffered(expandArea, queryShape.getContext());
					}
					return SpatialPredicate.IsWithin.evaluate(entityShape, queryShape);
				} else {
					return false;
				}
			}

		}
		return false;
	}

	public void checkSubscriptionsWithDelta(BaseRequest appendRequest, long messageTime, int messageType) {
		String id = appendRequest.getId();
		String tenantId = appendRequest.getTenant();
		Set<String> types = getTypesForId(tenantId, id);
		ArrayList<SubscriptionRequest> subsToCheck = new ArrayList<SubscriptionRequest>();
		List<SubscriptionRequest> subs = getAllTypeBaseRequests(tenantId, types);
		if (subs != null) {
			for (SubscriptionRequest sub : subs) {
				for (EntityInfo entityInfo : sub.getSubscription().getEntities()) {
					if (entityInfo.getId() == null && entityInfo.getIdPattern() == null) {
						subsToCheck.add(sub);
						break;
					}
					if (entityInfo.getId() != null && entityInfo.getId().toString().equals(id)) {
						subsToCheck.add(sub);
						break;
					}
					if (entityInfo.getIdPattern() != null && id.matches(entityInfo.getIdPattern())) {
						subsToCheck.add(sub);
						break;
					}
				}
			}
		}
		addAllTypeSubscriptions(appendRequest.getHeaders(), subsToCheck);
		checkSubscriptions(subsToCheck, appendRequest, messageType, messageTime);

	}

	// @StreamListener(SubscriptionManagerConsumerChannel.deleteReadChannel)

	private List<SubscriptionRequest> getAllTypeBaseRequests(String tenant, Set<String> types) {
		ArrayList<SubscriptionRequest> subs = new ArrayList<SubscriptionRequest>();
		for (String type : types) {
			List<SubscriptionRequest> tmp = this.type2EntitiesSubscriptions.get(tenant, type);
			if (tmp != null) {
				subs.addAll(tmp);
			}
		}
		return subs;
	}

	private Set<String> getTypesForId(String tenantId, String entityId) {
		synchronized (this.tenant2Ids2Type) {
			Set<String> result = this.tenant2Ids2Type.get(tenantId, entityId);
			if (result == null) {
				return new HashSet<String>();
			}
			return result;
		}
	}

	class CancelTask extends TimerTask {

		private SubscriptionRequest request;

		public CancelTask(SubscriptionRequest request) {
			this.request = request;
		}

		@Override
		public void run() {
			try {
				unsubscribe(request.getSubscription().getId(), request.getHeaders());
			} catch (ResponseException e) {
				logger.error("Failed to unsubscribed timed subscription", e);
			}
		}
	}
}
