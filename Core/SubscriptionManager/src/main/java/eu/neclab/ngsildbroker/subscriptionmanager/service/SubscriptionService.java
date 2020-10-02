package eu.neclab.ngsildbroker.subscriptionmanager.service;

import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_CONTAINS;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_DISJOINT;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_EQUALS;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_INTERSECTS;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_NEAR;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_OVERLAPS;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_WITHIN;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.spatial4j.SpatialPredicate;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory.PolygonBuilder;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.filosganga.geogson.model.Point;
import com.github.filosganga.geogson.model.Polygon;
import com.github.filosganga.geogson.model.positions.SinglePosition;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonParseException;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.shared.Application;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BaseProperty;
import eu.neclab.ngsildbroker.commons.datatypes.EndPoint;
import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.GeoProperty;
import eu.neclab.ngsildbroker.commons.datatypes.GeoPropertyEntry;
import eu.neclab.ngsildbroker.commons.datatypes.LDGeoQuery;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.Format;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.NotificationHandler;
import eu.neclab.ngsildbroker.commons.interfaces.SubscriptionManager;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;

@Service
public class SubscriptionService implements SubscriptionManager {
//TODO Change notification data generation so that always the changed value from kafka is definatly present in the notification not the DB version(that one could have been already updated)

	private final static Logger logger = LogManager.getLogger(SubscriptionService.class);

	private final int CREATE = 0;
	private final int APPEND = 1;
	private final int UPDATE = 2;
	private final int DELETE = 3;

	private final String ALL_TYPES_TYPE = "()";

	@Value("${atcontext.url}")
	String atContextServerUrl;

	NotificationHandlerREST notificationHandlerREST;
	IntervalNotificationHandler intervalHandlerREST;

	NotificationHandlerMQTT notificationHandlerMQTT;
	IntervalNotificationHandler intervalHandlerMQTT;

	Timer watchDog = new Timer(true);

	// KafkaOps kafkaOps = new KafkaOps();

	@Autowired
	ObjectMapper objectMapper;

	@Autowired
	@Qualifier("smconRes")
	ContextResolverBasic contextResolverService;

	@Autowired
	EurekaClient eurekaClient;

	@Autowired
	@Qualifier("smqueryParser")
	QueryParser queryParser;

	@Autowired
	ReplyingKafkaTemplate<String, byte[], byte[]> kafkaTemplate;

	@Autowired
	SubscriptionInfoDAO subscriptionInfoDAO;

	@Value("${query.topic}")
	String requestTopic;

	@Value("${query.result.topic}")
	String queryResultTopic;

	@Autowired
	@Qualifier("smparamsResolver")
	ParamsResolver paramsResolver;

	boolean directDB = true;

	JtsShapeFactory shapeFactory = JtsSpatialContext.GEO.getShapeFactory();

	HashMap<String, Subscription> subscriptionId2Subscription = new HashMap<String, Subscription>();
	HashMap<String, TimerTask> subId2TimerTask = new HashMap<String, TimerTask>();
	ArrayListMultimap<String, Subscription> type2EntitiesSubscriptions = ArrayListMultimap.create();
	HashMap<Subscription, Long> sub2CreationTime = new HashMap<Subscription, Long>();
	ArrayListMultimap<String, Object> subscriptionId2Context = ArrayListMultimap.create();
	HashMap<String, Subscription> remoteNotifyCallbackId2InternalSub = new HashMap<String, Subscription>();
	@Value("${bootstrap.servers}")
	String BOOTSTRAP_SERVERS;

	HttpUtils httpUtils;

	private Map<String, String> ids2Type;

	private HTreeMap<String,String> subscriptionStore;
	@Value("${subscriptions.store:subscriptionstore.db}")
	private String subscriptionStoreLocation;

	// @Value("${notification.port}")
	// String REMOTE_NOTIFICATION_PORT;

	public SubscriptionService() {
	}

	@PostConstruct
	private void setup() {
		this.ids2Type = subscriptionInfoDAO.getIds2Type();
		httpUtils = HttpUtils.getInstance(contextResolverService);
		notificationHandlerREST = new NotificationHandlerREST(this, contextResolverService, objectMapper);
		intervalHandlerREST = new IntervalNotificationHandler(notificationHandlerREST, kafkaTemplate, queryResultTopic,
				requestTopic, paramsResolver);
		notificationHandlerMQTT = new NotificationHandlerMQTT(this, contextResolverService, objectMapper);
		intervalHandlerMQTT = new IntervalNotificationHandler(notificationHandlerMQTT, kafkaTemplate, queryResultTopic,
				requestTopic, paramsResolver);
		logger.trace("call loadStoredSubscriptions() ::");
		this.subscriptionStore = DBMaker.fileDB(this.subscriptionStoreLocation).closeOnJvmShutdown().checksumHeaderBypass().transactionEnable().make().hashMap("subscriptions", Serializer.STRING, Serializer.STRING).createOrOpen();
		loadStoredSubscriptions();

	}
	@PreDestroy
	private void deconstructor() {
		subscriptionStore.close();
	}
	private void loadStoredSubscriptions() {
		// TODO Auto-generated method stub
		synchronized (this.subscriptionStore) {
			for (Entry<String, String> entry : subscriptionStore.entrySet()) {
				try {
					SubscriptionRequest subscription = DataSerializer.getSubscriptionRequest(entry.getValue());
					subscribe(subscription);
				} catch (JsonParseException e) {
					logger.error("Exception ::", e);
					e.printStackTrace();
					continue;
				} catch (ResponseException e) {
					logger.error("Exception ::", e);
					e.printStackTrace();
					continue;
				}
			}
			
		}
	}

	@Override
	public URI subscribe(SubscriptionRequest subscriptionRequest) throws ResponseException {
		logger.debug("Subscribe got called " + subscriptionRequest.getSubscription().toString());
		Subscription subscription = subscriptionRequest.getSubscription();
		validateSub(subscription);
		if (subscription.getId() == null) {
			subscription.setId(generateUniqueSubId(subscription));
		} else {
			if (this.subscriptionId2Subscription.containsKey(subscription.getId().toString())) {
				throw new ResponseException(ErrorType.AlreadyExists);
			}
		}

		this.subscriptionId2Subscription.put(subscription.getId().toString(), subscription);
		if (subscription.getLdQuery() != null && !subscription.getLdQuery().trim().equals("")) {
			subscription
					.setQueryTerm(queryParser.parseQuery(subscription.getLdQuery(), subscriptionRequest.getContext()));
		}
		String endpointProtocol = subscription.getNotification().getEndPoint().getUri().getScheme();
		if (subscription.getTimeInterval() > 0) {
			if (endpointProtocol.equals("mqtt")) {
				intervalHandlerMQTT.addSub(subscriptionRequest);
			} else {
				intervalHandlerREST.addSub(subscriptionRequest);
			}
		} else {
			this.subscriptionId2Context.putAll(subscription.getId().toString(), subscriptionRequest.getContext());
			this.sub2CreationTime.put(subscription, System.currentTimeMillis());
			List<EntityInfo> entities = subscription.getEntities();
			if (entities == null || entities.isEmpty()) {
				this.type2EntitiesSubscriptions.put(ALL_TYPES_TYPE, subscription);
			} else {
				for (EntityInfo info : subscription.getEntities()) {
					this.type2EntitiesSubscriptions.put(info.getType(), subscription);

				}

			}
			storeSubscription(subscriptionRequest);

			if (subscription.getExpires() != null) {
				TimerTask cancel = new TimerTask() {

					@Override
					public void run() {
						try {
							unsubscribe(subscription.getId());
						} catch (ResponseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				};
				subId2TimerTask.put(subscription.getId().toString(), cancel);
				watchDog.schedule(cancel, subscription.getExpires() - System.currentTimeMillis());
			}
		}
		return subscription.getId();
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

	}

	private void storeSubscription(SubscriptionRequest subscription) throws ResponseException {
		new Thread() {
			public void run() {
				synchronized (subscriptionStore) {
					subscriptionStore.put(subscription.getSubscription().getId().toString(), DataSerializer.toJson(subscription));
				}
			};
		}.start();

	}

	private URI generateUniqueSubId(Subscription subscription) {

		try {
			return new URI("urn:ngsi-ld:Subscription:" + subscription.hashCode());
		} catch (URISyntaxException e) {
			// Left empty intentionally should never happen
			throw new AssertionError();
		}
	}

	@Override
	public void unsubscribe(URI id) throws ResponseException {
		Subscription removedSub;
		synchronized (subscriptionId2Subscription) {
			removedSub = this.subscriptionId2Subscription.remove(id.toString());
		}

		if (removedSub == null) {
			throw new ResponseException(ErrorType.NotFound);
		}
		synchronized (subscriptionId2Context) {
			this.subscriptionId2Context.removeAll(id.toString());
		}
		intervalHandlerREST.removeSub(id.toString());
		intervalHandlerMQTT.removeSub(id.toString());
		List<EntityInfo> entities = removedSub.getEntities();
		if (entities == null || entities.isEmpty()) {
			synchronized (type2EntitiesSubscriptions) {
				type2EntitiesSubscriptions.remove(ALL_TYPES_TYPE, removedSub);
			}
		} else {
			for (EntityInfo info : entities) {
				synchronized (type2EntitiesSubscriptions) {
					type2EntitiesSubscriptions.remove(info.getType(), removedSub);
				}
			}
		}
		TimerTask task = subId2TimerTask.get(id.toString());
		if (task != null) {
			task.cancel();
		}
		// TODO remove remote subscription
		new Thread() {
			public void run() {
				synchronized (subscriptionStore) {
					subscriptionStore.remove(id.toString());
				}
			};
		}.start();

	}

	@Override
	public Subscription updateSubscription(SubscriptionRequest subscriptionRequest) throws ResponseException {
		Subscription subscription = subscriptionRequest.getSubscription();
		Subscription oldSub = subscriptionId2Subscription.get(subscription.getId().toString());

		if (oldSub == null) {
			throw new ResponseException(ErrorType.NotFound);
		}
		if (subscription.getAttributeNames() != null) {
			oldSub.setAttributeNames(subscription.getAttributeNames());
		}
		if (subscription.getDescription() != null) {
			oldSub.setDescription(subscription.getDescription());
		}
		if (subscription.getEntities() != null && !subscription.getEntities().isEmpty()) {
			oldSub.setEntities(subscription.getEntities());
		}
		if (subscription.getExpires() != null) {
			oldSub.setExpires(subscription.getExpires());
			synchronized (subId2TimerTask) {
				TimerTask task = subId2TimerTask.get(oldSub.getId().toString());
				task.cancel();
				watchDog.schedule(task, subscription.getExpires() - System.currentTimeMillis());
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
		synchronized (this.subscriptionId2Context) {
			this.subscriptionId2Context.putAll(oldSub.getId().toString(), subscriptionRequest.getContext());
		}
		return oldSub;
	}

	@Override
	public List<Subscription> getAllSubscriptions(int limit) {
		List<Subscription> result = new ArrayList<Subscription>();
		result.addAll(subscriptionId2Subscription.values());
		if (limit > 0) {
			if (limit < result.size()) {
				result = result.subList(0, limit);
			}
		}
		return result;
	}

	@Override
	public Subscription getSubscription(String subscriptionId) throws ResponseException {
		if (subscriptionId2Subscription.containsKey(subscriptionId)) {
			return subscriptionId2Subscription.get(subscriptionId);
		} else {
			throw new ResponseException(ErrorType.NotFound);
		}

	}

	@KafkaListener(topics = "${entity.create.topic}", groupId = "submanager")
	public void handleCreate(Message<byte[]> message) {
		String payload = new String(message.getPayload());
		String key = KafkaOps.getMessageKey(message);
		logger.debug("Create got called: " + payload);
		logger.debug(key);
		checkSubscriptionsWithCreate(key, payload, (long) message.getHeaders().get(KafkaHeaders.RECEIVED_TIMESTAMP));
	}

	private void checkSubscriptionsWithCreate(String key, String payload, long messageTime) {
		Entity create = DataSerializer.getEntity(payload);
		synchronized (this.ids2Type) {
			this.ids2Type.put(key, create.getType());
		}

		ArrayList<Subscription> subsToCheck = new ArrayList<Subscription>();
		for (Subscription sub : this.type2EntitiesSubscriptions.get(create.getType())) {
			for (EntityInfo entityInfo : sub.getEntities()) {
				if (entityInfo.getId() == null && entityInfo.getIdPattern() == null) {
					subsToCheck.add(sub);
					break;
				}
				if (entityInfo.getId() != null && entityInfo.getId().toString().equals(key)) {
					subsToCheck.add(sub);
					break;
				}
				if (entityInfo.getIdPattern() != null && key.matches(entityInfo.getIdPattern())) {
					subsToCheck.add(sub);
					break;
				}
			}
		}
		subsToCheck.addAll(this.type2EntitiesSubscriptions.get(ALL_TYPES_TYPE));
		checkSubscriptions(subsToCheck, create, CREATE, messageTime);

	}

	private void checkSubscriptions(ArrayList<Subscription> subsToCheck, Entity entity, int methodType,
			long messageTime) {

		for (Subscription subscription : subsToCheck) {
			if (messageTime >= sub2CreationTime.get(subscription)) {
				new Thread() {
					public void run() {
						Entity data = null;
						try {
							switch (methodType) {
							case CREATE:
								data = generateNotificationEntity(entity, subscription);

								break;
							case APPEND:
								data = generateDataFromBaseOp(entity, subscription);
								break;
							case UPDATE:
								data = generateDataFromBaseOp(entity, subscription);
								break;
							case DELETE:

								break;

							default:
								break;
							}

							if (data != null) {
								ArrayList<Entity> dataList = new ArrayList<Entity>();
								dataList.add(data);
								sendNotification(dataList, subscription);
							}
						} catch (ResponseException e) {
							logger.error("Failed to handle new data for the subscriptions, cause: " + e.getMessage());
						}
					}

				}.start();
			}

		}

	}

	private void sendNotification(List<Entity> dataList, Subscription subscription) {
		logger.debug(DataSerializer.toJson(dataList));
		// System.out.println("SENDING NOTIFICATION: " + DataSerializer.toJson(dataList)
		// + " \nTO SUBSCRIPTION \n"
		// + DataSerializer.toJson(subscription));
		// if (subscription.getTimeInterval() > 0) {
		// try {
		// intervalHandler.notify(new
		// Notification(EntityTools.getRandomID("notification:"),
		// System.currentTimeMillis(), subscription.getId(), dataList, null, null, 0,
		// true),
		// subscription.getId().toString());
		// } catch (URISyntaxException e) {
		// logger.error("Exception ::", e);
		// // Left empty intentionally
		// throw new AssertionError();
		// }
		// } else {
		try {
			String endpointProtocol = subscription.getNotification().getEndPoint().getUri().getScheme();

			NotificationHandler handler;
			if (endpointProtocol.equals("mqtt")) {
				handler = notificationHandlerMQTT;
			} else {
				handler = notificationHandlerREST;
			}
			handler.notify(
					new Notification(EntityTools.getRandomID("notification:"), System.currentTimeMillis(),
							subscription.getId(), dataList, null, null, 0, true),
					subscription.getNotification().getEndPoint().getUri(),
					subscription.getNotification().getEndPoint().getAccept(), subscription.getId().toString(),
					subscriptionId2Context.get(subscription.getId().toString()), subscription.getThrottling(),
					subscription.getNotification().getEndPoint().getNotifierInfo());
		} catch (URISyntaxException e) {
			logger.error("Exception ::", e);
			// Left empty intentionally
			throw new AssertionError();
		}
		// }
	}

	private Entity generateNotificationEntity(Entity entity, Subscription subscription) throws ResponseException {

		if (!evaluateGeoQuery(subscription.getLdGeoQuery(), entity.getLocation())) {
			return null;
		}
		if (subscription.getQueryTerm() != null) {
			if (!subscription.getQueryTerm().calculate(entity.getAllBaseProperties())) {
				return null;
			}
		}
		List<BaseProperty> baseProps = extractBaseProps(entity, subscription);

		if (baseProps.isEmpty()) {
			return null;
		}
		Entity result = new Entity(entity.getId(), entity.getType(), baseProps, entity.getRefToAccessControl());

		return result;
	}

	private List<BaseProperty> extractBaseProps(Entity entity, Subscription subscription) {
		ArrayList<BaseProperty> result = new ArrayList<BaseProperty>();
		if (!shouldFire(entity, subscription)) {
			return result;
		}
		ArrayList<String> attribNames = getAttribNames(subscription);
		if (attribNames.isEmpty()) {
			return entity.getAllBaseProperties();
		}

		for (BaseProperty property : entity.getAllBaseProperties()) {
			if (attribNames.contains(property.getIdString())) {
				result.add(property);
			}
		}
		return result;
	}

	private boolean shouldFire(Entity entity, Subscription subscription) {
		if (subscription.getAttributeNames() == null || subscription.getAttributeNames().isEmpty()) {
			return true;
		}
		for (String attribName : subscription.getAttributeNames()) {
			for (BaseProperty baseProp : entity.getAllBaseProperties()) {
				if (attribName.equals(baseProp.getIdString())) {
					return true;
				}
			}
		}
		return false;
	}

	private Entity generateDataFromBaseOp(Entity deltaInfo, Subscription subscription) throws ResponseException {
		String entityBody = null;
		if (!shouldFire(deltaInfo, subscription)) {
			return null;
		}

		if (directDB) {
			entityBody = subscriptionInfoDAO.getEntity(deltaInfo.getId().toString());
		}
		// HERE YOU NEED TO REPLACE THE ATTRIBUTE TO THE ONE FROM DELTA
		Entity entity = DataSerializer.getEntity(entityBody);
		if (!evaluateGeoQuery(subscription.getLdGeoQuery(), entity.getLocation())) {
			return null;
		}
		if (subscription.getQueryTerm() != null) {
			if (!subscription.getQueryTerm().calculate(entity.getAllBaseProperties())) {
				return null;
			}
		}

		List<BaseProperty> baseProps = extractBaseProps(entity, subscription);
		if (baseProps.isEmpty()) {
			return null;
		}
		Entity temp = new Entity(deltaInfo.getId(), entity.getType(), baseProps, entity.getRefToAccessControl());

		return temp;
	}

	private ArrayList<String> getAttribNames(Subscription subscription) {
		ArrayList<String> attribNames = new ArrayList<String>();
		if (subscription.getNotification().getAttributeNames() != null) {
			attribNames.addAll(subscription.getNotification().getAttributeNames());
		}
		// if (subscription.getAttributeNames() != null) {
		// attribNames.addAll(subscription.getAttributeNames());
		// }
		return attribNames;
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
						polygonBuilder.pointXY(((SinglePosition) it2).coordinates().getLon(),
								((SinglePosition) it2).coordinates().getLat());
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

	// private Property getPropertyByName(String name, List<Property> properties) {
	// for (Property property : properties) {
	// if (property.getName().equals(name)) {
	// return property;
	// }
	// }
	// return null;
	// }

	@KafkaListener(topics = "${entity.update.topic}", groupId = "submanager")
	public void handleUpdate(Message<byte[]> message) {
		String payload = new String(message.getPayload());
		String key = KafkaOps.getMessageKey(message);
		logger.debug("update got called: " + payload);
		logger.debug(key);
		checkSubscriptionsWithUpdate(key, payload, (long) message.getHeaders().get(KafkaHeaders.RECEIVED_TIMESTAMP));
	}

	private void checkSubscriptionsWithUpdate(String key, String payload, long messageTime) {
		Entity update = DataSerializer.getPartialEntity(payload);
		String type = getTypeForId(key);
		try {
			update.setId(new URI(key));
		} catch (URISyntaxException e) {
			// left empty intentionally should never happen because the uri should be
			// already checked
			e.printStackTrace();
		}
		update.setType(type);
		ArrayList<Subscription> subsToCheck = new ArrayList<Subscription>();
		for (Subscription sub : this.type2EntitiesSubscriptions.get(type)) {
			for (EntityInfo entityInfo : sub.getEntities()) {
				if (entityInfo.getId() == null && entityInfo.getIdPattern() == null) {
					subsToCheck.add(sub);
					break;
				}
				if (entityInfo.getId() != null && entityInfo.getId().toString().equals(key)) {
					subsToCheck.add(sub);
					break;
				}
				if (entityInfo.getIdPattern() != null && key.matches(entityInfo.getIdPattern())) {
					subsToCheck.add(sub);
					break;
				}
			}
		}
		subsToCheck.addAll(this.type2EntitiesSubscriptions.get(ALL_TYPES_TYPE));
		checkSubscriptions(subsToCheck, update, UPDATE, messageTime);

	}

	@KafkaListener(topics = "${entity.append.topic}", groupId = "submanager")
	public void handleAppend(Message<byte[]> message) {
		String payload = new String(message.getPayload());
		String key = KafkaOps.getMessageKey(message);
		logger.debug("Create got called: " + payload);
		logger.debug(key);
		checkSubscriptionsWithAppend(key, payload, (long) message.getHeaders().get(KafkaHeaders.RECEIVED_TIMESTAMP));
	}

	private void checkSubscriptionsWithAppend(String key, String payload, long messageTime) {
		Entity append = DataSerializer.getPartialEntity(payload);
		String type = getTypeForId(key);
		try {
			append.setId(new URI(key));
		} catch (URISyntaxException e) {
			// left empty intentionally should never happen because the uri should be
			// already checked
			e.printStackTrace();
		}
		append.setType(type);
		ArrayList<Subscription> subsToCheck = new ArrayList<Subscription>();
		for (Subscription sub : this.type2EntitiesSubscriptions.get(type)) {
			for (EntityInfo entityInfo : sub.getEntities()) {
				if (entityInfo.getId() == null && entityInfo.getIdPattern() == null) {
					subsToCheck.add(sub);
					break;
				}
				if (entityInfo.getId() != null && entityInfo.getId().toString().equals(key)) {
					subsToCheck.add(sub);
					break;
				}
				if (entityInfo.getIdPattern() != null && key.matches(entityInfo.getIdPattern())) {
					subsToCheck.add(sub);
					break;
				}
			}
		}
		subsToCheck.addAll(this.type2EntitiesSubscriptions.get(ALL_TYPES_TYPE));
		checkSubscriptions(subsToCheck, append, APPEND, messageTime);

	}

	// @StreamListener(SubscriptionManagerConsumerChannel.deleteReadChannel)
	@KafkaListener(topics = "${entity.delete.topic}", groupId = "submanager")
	public void handleDelete(Message<byte[]> message) throws Exception {
		this.ids2Type.remove(KafkaOps.getMessageKey(message));
		// checkSubscriptionsWithDelete(new String((byte[])
		// message.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY)),
		// new String(message.getPayload()));
	}

	@KafkaListener(topics = "${csource.notification.topic}", groupId = "submanager")
	public void handleCSourceNotification(Message<byte[]> message) {
		String payload = new String(message.getPayload());
		String key = KafkaOps.getMessageKey(message);
		@SuppressWarnings("unchecked")
		ArrayList<String> endPoints = DataSerializer.getStringList(payload);
		subscribeToRemote(subscriptionId2Subscription.get(key), endPoints);
	}

	// @KafkaListener(topics = "${csource.registry.topic}", groupId = "submanager")
	// public void handleCSourceRegistry(Message<byte[]> message) throws Exception {
	// CSourceRegistration csourceRegistration = objectMapper.readValue((byte[])
	// message.getPayload(),
	// CSourceRegistration.class);
	// checkSubscriptionsWithCSource(csourceRegistration);
	// }

	private void subscribeToRemote(Subscription sub, ArrayList<String> remoteEndPoints) {
		new Thread() {
			@Override
			public void run() {

				Subscription remoteSub = new Subscription();
				remoteSub.setCustomFlags(sub.getCustomFlags());
				remoteSub.setDescription(sub.getDescription());
				remoteSub.setEntities(sub.getEntities());
				remoteSub.setExpires(sub.getExpires());
				remoteSub.setLdGeoQuery(sub.getLdGeoQuery());
				remoteSub.setLdQuery(sub.getLdQuery());
				remoteSub.setLdTempQuery(sub.getLdTempQuery());
				remoteSub.setName(sub.getName());
				remoteSub.setStatus(sub.getStatus());
				remoteSub.setThrottling(sub.getThrottling());
				remoteSub.setTimeInterval(sub.getTimeInterval());
				remoteSub.setType(sub.getType());
				NotificationParam remoteNotification = new NotificationParam();
				remoteNotification.setAttributeNames(sub.getNotification().getAttributeNames());
				remoteNotification.setFormat(Format.normalized);
				EndPoint endPoint = new EndPoint();
				endPoint.setAccept(AppConstants.NGB_APPLICATION_JSONLD);
				endPoint.setUri(prepareNotificationServlet(sub));
				remoteNotification.setEndPoint(endPoint);
				remoteSub.setAttributeNames(sub.getAttributeNames());
				String body = DataSerializer.toJson(remoteSub);
				HashMap<String, String> additionalHeaders = new HashMap<String, String>();
				additionalHeaders.put(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD);
				for (String remoteEndPoint : remoteEndPoints) {
					try {
						httpUtils.doPost(new URI(remoteEndPoint), body, additionalHeaders);
					} catch (IOException e) {
						// TODO what to do when a remote sub times out ? at the moment we just fail here
						e.printStackTrace();
					} catch (URISyntaxException e) {

						e.printStackTrace();
					}
				}

			}
		}.start();

	}

	private URI prepareNotificationServlet(Subscription subToCheck) {
		Application application = eurekaClient.getApplication("gateway");
		InstanceInfo instanceInfo = application.getInstances().get(0);
		// TODO : search for a better way to resolve http or https
		String hostIP = instanceInfo.getIPAddr();
		String uuid = Long.toString(UUID.randomUUID().getLeastSignificantBits());
		int port = instanceInfo.getPort();

		remoteNotifyCallbackId2InternalSub.put(uuid, subToCheck);
		StringBuilder url = new StringBuilder("http://").append(hostIP).append(":").append(port)
				.append("/remotenotify/").append(uuid);
		// System.out.println("URL : "+url.toString());
		try {
			return new URI(url.toString());
		} catch (URISyntaxException e) {
			logger.error("Exception ::", e);
			// should never happen
			return null;
		}

	}

	@SuppressWarnings("unused")
	// Kept for now ... Delete notifications are not present
	private void checkSubscriptionsWithDelete(String key, String payload, long messageTime) {
		Entity delete = DataSerializer.getEntity(payload);
		ArrayList<Subscription> subsToCheck = new ArrayList<Subscription>();
		/*
		 * subsToCheck.addAll(this.idBasedSubscriptions.get(key));
		 * subsToCheck.addAll(this.typeBasedSubscriptions.get(delete.getType()));
		 */
		checkSubscriptions(subsToCheck, delete, DELETE, messageTime);

	}

	private String getTypeForId(String key) {
		synchronized (this.ids2Type) {
			return (String) this.ids2Type.get(key);
		}
		/*
		 * //this has to be db handled byte[] json = kafkaOps.getMessage(key,
		 * KafkaConstants.ENTITY_TOPIC); if (json == null) { return ""; } try { return
		 * objectMapper.readTree(json).get(JSON_LD_TYPE).get(0).asText(""); } catch
		 * (IOException e) { logger.error("Exception ::", e); e.printStackTrace(); }
		 * return "";
		 */
	}

	@Override
	public void remoteNotify(String id, Notification notification) {
		new Thread() {
			@Override
			public void run() {
				Subscription subscription = remoteNotifyCallbackId2InternalSub.get(id);
				sendNotification(notification.getData(), subscription);
			}
		}.start();

	}

	public void reportNotification(String subId, Long now) {
		synchronized (subscriptionId2Subscription) {
			Subscription subscription = subscriptionId2Subscription.get(subId);
			if (subscription != null) {
				subscription.getNotification().setLastNotification(new Date(now));
				subscription.getNotification().setLastSuccessfulNotification(new Date(now));
			}
		}
	}

	public void reportFailedNotification(String subId, Long now) {
		synchronized (subscriptionId2Subscription) {
			Subscription subscription = subscriptionId2Subscription.get(subId);
			if (subscription != null) {
				subscription.getNotification().setLastFailedNotification(new Date(now));
			}
		}
	}

	public void reportSuccessfulNotification(String subId, Long now) {
		synchronized (subscriptionId2Subscription) {
			Subscription subscription = subscriptionId2Subscription.get(subId);
			if (subscription != null) {
				subscription.getNotification().setLastSuccessfulNotification(new Date(now));
			}
		}
	}

}
