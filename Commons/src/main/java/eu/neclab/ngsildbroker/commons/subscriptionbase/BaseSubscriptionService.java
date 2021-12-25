package eu.neclab.ngsildbroker.commons.subscriptionbase;

import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_CONTAINS;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_DISJOINT;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_EQUALS;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_INTERSECTS;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_NEAR;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_OVERLAPS;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_WITHIN;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.locationtech.spatial4j.SpatialPredicate;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory.PolygonBuilder;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClient;

import com.github.filosganga.geogson.model.Point;
import com.github.filosganga.geogson.model.Polygon;
import com.github.filosganga.geogson.model.positions.SinglePosition;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EndPoint;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.GeoProperty;
import eu.neclab.ngsildbroker.commons.datatypes.GeoPropertyEntry;
import eu.neclab.ngsildbroker.commons.datatypes.LDGeoQuery;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.Format;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.NotificationHandler;
import eu.neclab.ngsildbroker.commons.interfaces.SubscriptionCRUDService;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import reactor.core.publisher.Mono;

public abstract class BaseSubscriptionService implements SubscriptionCRUDService {

	private final static Logger logger = LoggerFactory.getLogger(BaseSubscriptionService.class);

	private final String ALL_TYPES_TYPE = "()";

	private NotificationHandlerREST notificationHandlerREST;
	private IntervalNotificationHandler intervalHandlerREST;

	private NotificationHandlerMQTT notificationHandlerMQTT;
	private IntervalNotificationHandler intervalHandlerMQTT;

	private Timer watchDog = new Timer(true);

	@Autowired
	@Qualifier("subwebclient")
	private WebClient webClient;

	private SubscriptionInfoDAOInterface subscriptionInfoDAO;

	private JtsShapeFactory shapeFactory = JtsSpatialContext.GEO.getShapeFactory();

	private Table<String, String, SubscriptionRequest> tenant2subscriptionId2Subscription = HashBasedTable.create();
	private Table<String, String, TimerTask> subId2TimerTask = HashBasedTable.create();
	private Table<String, String, List<SubscriptionRequest>> type2EntitiesSubscriptions = HashBasedTable.create();
	private HashMap<SubscriptionRequest, Long> sub2CreationTime = new HashMap<SubscriptionRequest, Long>();
	private Table<String, String, List<Object>> tenantId2subscriptionId2Context = HashBasedTable.create();
	private HashMap<String, SubscriptionRequest> remoteNotifyCallbackId2InternalSub = new HashMap<String, SubscriptionRequest>();
	private Table<String, String, Set<String>> tenant2Ids2Type;

	@PostConstruct
	private void setup() {
		subscriptionInfoDAO = getSubscriptionInfoDao();
		try {
			this.tenant2Ids2Type = subscriptionInfoDAO.getIds2Type();
		} catch (ResponseException e) {
			logger.error(e.getLocalizedMessage());
		}
		notificationHandlerREST = new NotificationHandlerREST(webClient);
		Subscription temp = new Subscription();
		temp.setId("invalid:base");
		intervalHandlerREST = new IntervalNotificationHandler(notificationHandlerREST, subscriptionInfoDAO,
				getNotification(new SubscriptionRequest(temp, null, null), null, AppConstants.UPDATE_REQUEST));

		notificationHandlerMQTT = new NotificationHandlerMQTT();
		intervalHandlerMQTT = new IntervalNotificationHandler(notificationHandlerMQTT, subscriptionInfoDAO,
				getNotification(new SubscriptionRequest(temp, null, null), null, AppConstants.UPDATE_REQUEST));
		logger.trace("call loadStoredSubscriptions() ::");
		loadStoredSubscriptions();

	}

	protected abstract SubscriptionInfoDAOInterface getSubscriptionInfoDao();

	@PreDestroy
	private void deconstructor() {
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
		validateSub(subscription);
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
		if (subscription.getLdQuery() != null && !subscription.getLdQuery().trim().equals("")) {
			subscription.setQueryTerm(QueryParser.parseQuery(subscription.getLdQuery(),
					JsonLdProcessor.getCoreContextClone().parse(subscriptionRequest.getContext(), true)));
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
			if (subscription.getExpiresAt() != null) {
				TimerTask cancel = new TimerTask() {

					@Override
					public void run() {
						try {
							unsubscribe(subscription.getId(), subscriptionRequest.getHeaders());
						} catch (ResponseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				};
				subId2TimerTask.put(subscriptionRequest.getTenant(), subscription.getId().toString(), cancel);
				watchDog.schedule(cancel, subscription.getExpiresAt() - System.currentTimeMillis());
			}
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

	private void validateSub(Subscription subscription) throws ResponseException {
		if (subscription.getThrottling() > 0 && subscription.getTimeInterval() > 0) {
			throw new ResponseException(ErrorType.BadRequestData, "throttling  and timeInterval cannot both be set");
		}
		if (subscription.getTimeInterval() > 0) {
			if (subscription.getAttributeNames() == null || subscription.getAttributeNames().isEmpty()) {
				return;
			}
			throw new ResponseException(ErrorType.BadRequestData,
					"watchedAttributes  and timeInterval cannot both be set");
		}
		if (subscription.getExpiresAt() != null && !isValidFutureDate(subscription.getExpiresAt())) {
			logger.error("Invalid expire date!");
			throw new ResponseException(ErrorType.BadRequestData, "Invalid expire date!");
		}

	}

	private String generateUniqueSubId(Subscription subscription) {
		return "urn:ngsi-ld:Subscription:" + subscription.hashCode();
	}

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
		// intervalHandlerREST.removeSub(id);
		// intervalHandlerMQTT.removeSub(id);
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

			if (subscription.getAttributeNames() != null) {
				oldSub.setAttributeNames(subscription.getAttributeNames());
			}
			if (subscription.getDescription() != null) {
				oldSub.setDescription(subscription.getDescription());
			}
			if (subscription.getEntities() != null && !subscription.getEntities().isEmpty()) {
				oldSub.setEntities(subscription.getEntities());
			}
			if (subscription.getExpiresAt() != null) {
				oldSub.setExpiresAt(subscription.getExpiresAt());
				synchronized (subId2TimerTask) {
					TimerTask task = subId2TimerTask.get(subscriptionRequest.getTenant(), oldSub.getId().toString());
					task.cancel();
					watchDog.schedule(task, subscription.getExpiresAt() - System.currentTimeMillis());
				}

			}
			if (subscription.getLdGeoQuery() != null) {
				oldSub.setLdGeoQuery(subscription.getLdGeoQuery());
			}
			if (subscription.getLdQuery() != null) {
				oldSub.setLdQuery(subscription.getLdQuery());
			}
			if (subscription.getLdTempQuery() != null) {
				oldSub.setLdTempQuery(subscription.getLdTempQuery());
			}
			if (subscription.getNotification() != null) {
				oldSub.setNotification(subscription.getNotification());
			}
			if (subscription.getThrottling() != 0) {
				oldSub.setThrottling(subscription.getThrottling());
			}
			if (subscription.getTimeInterval() != 0) {
				oldSub.setTimeInterval(subscription.getTimeInterval());
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

	public void checkSubscriptionsWithAbsolute(BaseRequest createRequest, long messageTime, int messageType) {

		String id = createRequest.getId();
		Set<String> types = getTypesFromEntry(createRequest);
		synchronized (this.tenant2Ids2Type) {
			this.tenant2Ids2Type.put(createRequest.getTenant(), id, types);
		}
		String tenantId = createRequest.getId();
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
		addAllTypeSubscriptions(createRequest.getHeaders(), subsToCheck);
		checkSubscriptions(subsToCheck, createRequest, messageType, messageTime);

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

	private void sendNotification(List<Map<String, Object>> dataList, SubscriptionRequest subscription,
			int triggerReason) {

		String endpointProtocol = subscription.getSubscription().getNotification().getEndPoint().getUri().getScheme();

		NotificationHandler handler;
		if (endpointProtocol.equals("mqtt")) {
			handler = notificationHandlerMQTT;
		} else {
			handler = notificationHandlerREST;
		}
		handler.notify(getNotification(subscription, dataList, triggerReason), subscription);

	}

	protected abstract Notification getNotification(SubscriptionRequest request, List<Map<String, Object>> dataList,
			int triggerReason);

	private boolean shouldFire(Map<String, Object> entry, SubscriptionRequest subscription) {

		if (subscription.getSubscription().getAttributeNames() == null
				|| subscription.getSubscription().getAttributeNames().isEmpty()) {
			return true;
		}
		Set<String> keys = entry.keySet();
		for (String attribName : subscription.getSubscription().getAttributeNames()) {
			if (keys.contains(attribName)) {
				return true;
			}
		}
		return false;
	}

	private Map<String, Object> generateDataFromBaseOp(BaseRequest request, SubscriptionRequest subscription,
			int methodType) throws ResponseException {
		Map<String, Object> deltaInfo = request.getRequestPayload();
		if (!shouldFire(deltaInfo, subscription)) {
			return null;
		}
		Map<String, Object> fullEntry = request.getFinalPayload();
		if (!evaluateGeoQuery(subscription.getSubscription().getLdGeoQuery(),
				EntityTools.getLocation(fullEntry, subscription.getSubscription().getLdGeoQuery()))) {
			return null;
		}
		if (subscription.getSubscription().getQueryTerm() != null) {
			if (!subscription.getSubscription().getQueryTerm().calculate(EntityTools.getBaseProperties(fullEntry))) {
				return null;
			}
		}
		return EntityTools.clearBaseProps(fullEntry, subscription);
	}

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
				} else {
					logger.error("Unsupported GeoJson type. Currently Point and Polygon are supported.");
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
						Shape bufferedShape = queryShape.getBuffered(geoQuery.getGeoRelation().getMaxDistanceAsDouble(),
								queryShape.getContext());
						return SpatialPredicate.IsWithin.evaluate(entityShape, bufferedShape);
					} else if (geoQuery.getGeoRelation().getMinDistance() != null) {
						Shape bufferedShape = queryShape.getBuffered(geoQuery.getGeoRelation().getMinDistanceAsDouble(),
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

	public void subscribeToRemote(SubscriptionRequest subscriptionRequest, ArrayList<String> remoteEndPoints) {
		new Thread() {
			@Override
			public void run() {

				Subscription remoteSub = new Subscription();
				Subscription subscription = subscriptionRequest.getSubscription();

				remoteSub.setDescription(subscription.getDescription());
				remoteSub.setEntities(subscription.getEntities());
				remoteSub.setExpiresAt(subscription.getExpiresAt());
				remoteSub.setLdGeoQuery(subscription.getLdGeoQuery());
				remoteSub.setLdQuery(subscription.getLdQuery());
				remoteSub.setLdTempQuery(subscription.getLdTempQuery());
				remoteSub.setSubscriptionName(subscription.getSubscriptionName());
				remoteSub.setStatus(subscription.getStatus());
				remoteSub.setThrottling(subscription.getThrottling());
				remoteSub.setTimeInterval(subscription.getTimeInterval());
				remoteSub.setType(subscription.getType());
				NotificationParam remoteNotification = new NotificationParam();
				remoteNotification.setAttributeNames(subscription.getNotification().getAttributeNames());
				remoteNotification.setFormat(Format.normalized);
				EndPoint endPoint = new EndPoint();
				endPoint.setAccept(AppConstants.NGB_APPLICATION_JSONLD);
				endPoint.setUri(prepareNotificationServlet(subscriptionRequest));
				remoteNotification.setEndPoint(endPoint);
				remoteSub.setAttributeNames(subscription.getAttributeNames());
				String body = DataSerializer.toJson(remoteSub);
				HttpHeaders additionalHeaders = new HttpHeaders();
				additionalHeaders.add(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD);
				for (String remoteEndPoint : remoteEndPoints) {

					StringBuilder temp = new StringBuilder(remoteEndPoint);
					if (remoteEndPoint.endsWith("/")) {
						temp.deleteCharAt(remoteEndPoint.length() - 1);
					}
					temp.append(AppConstants.SUBSCRIPTIONS_URL);
					webClient.post().uri(temp.toString()).headers(httpHeadersOnWebClientBeingBuilt -> {
						httpHeadersOnWebClientBeingBuilt.addAll(additionalHeaders);
					}).bodyValue(body).exchangeToMono(response -> {
						if (response.statusCode().equals(HttpStatus.OK)) {
							return Mono.just(Void.class);
						} else {
							return response.createException().flatMap(Mono::error);
						}
					}).subscribe();

				}

			}
		}.start();

	}

	private URI prepareNotificationServlet(SubscriptionRequest subToCheck) {

		String uuid = Long.toString(UUID.randomUUID().getLeastSignificantBits());
		remoteNotifyCallbackId2InternalSub.put(uuid, subToCheck);
		StringBuilder url = new StringBuilder(MicroServiceUtils.getGatewayURL().toString()).append("/remotenotify/")
				.append(uuid);
		try {
			return new URI(url.toString());
		} catch (URISyntaxException e) {
			logger.error("Exception ::", e);
			// should never happen
			return null;
		}

	}

	private Set<String> getTypesForId(String tenantId, String entityId) {
		synchronized (this.tenant2Ids2Type) {

			Set<String> result = this.tenant2Ids2Type.get(tenantId, entityId);
			if (result == null) {
				System.err.println();
			}
			return result;
		}
	}

	public void remoteNotify(String id, List<Object> list) {
		// TODO maybe remove
		new Thread() {
			@Override
			public void run() {
				SubscriptionRequest subscription = remoteNotifyCallbackId2InternalSub.get(id);
				// sendNotification(list, subscription);
			}
		}.start();

	}

	// return true for future date validation
	private boolean isValidFutureDate(Long date) {
		return System.currentTimeMillis() < date;
	}
}
