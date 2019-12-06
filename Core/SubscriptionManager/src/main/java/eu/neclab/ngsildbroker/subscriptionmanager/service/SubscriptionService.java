package eu.neclab.ngsildbroker.subscriptionmanager.service;

import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_CONTAINS;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_DISJOINT;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_EQUALS;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_INTERSECTS;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_NEAR;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_OVERLAPS;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.GEO_REL_WITHIN;
import static eu.neclab.ngsildbroker.commons.constants.NGSIConstants.JSON_LD_TYPE;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.util.Arrays;
import org.locationtech.spatial4j.SpatialPredicate;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory.PolygonBuilder;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.kafka.annotation.KafkaListener;
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
import eu.neclab.ngsildbroker.commons.constants.KafkaConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BaseProperty;
import eu.neclab.ngsildbroker.commons.datatypes.EndPoint;
import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.GeoProperty;
import eu.neclab.ngsildbroker.commons.datatypes.LDGeoQuery;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.Property;
import eu.neclab.ngsildbroker.commons.datatypes.Relationship;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.Format;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.NotificationHandler;
import eu.neclab.ngsildbroker.commons.interfaces.SubscriptionManager;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.subscriptionmanager.config.SubscriptionManagerProducerChannel;

@Service
public class SubscriptionService implements SubscriptionManager {

	private final static Logger logger = LogManager.getLogger(SubscriptionService.class);

	private final int CREATE = 0;
	private final int APPEND = 1;
	private final int UPDATE = 2;
	private final int DELETE = 3;

	@Value("${atcontext.url}")
	String atContextServerUrl;

	NotificationHandler notificationHandler;
	IntervalNotificationHandler intervalHandler;

	Timer watchDog = new Timer(true);

	// KafkaOps kafkaOps = new KafkaOps();
	@Autowired
	@Qualifier("smops")
	KafkaOps kafkaOps;

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

	private final SubscriptionManagerProducerChannel producerChannel;

	JtsShapeFactory shapeFactory = JtsSpatialContext.GEO.getShapeFactory();

	HashMap<String, Subscription> subscriptionId2Subscription = new HashMap<String, Subscription>();
	HashMap<String, TimerTask> subId2TimerTask = new HashMap<String, TimerTask>();
	ArrayListMultimap<String, Subscription> idBasedSubscriptions = ArrayListMultimap.create();
	ArrayListMultimap<String, Subscription> typeBasedSubscriptions = ArrayListMultimap.create();
	ArrayListMultimap<String, Subscription> idPatternBasedSubscriptions = ArrayListMultimap.create();
	HashMap<Subscription, Long> sub2CreationTime = new HashMap<Subscription, Long>();
	ArrayListMultimap<String, Object> subscriptionId2Context = ArrayListMultimap.create();
	HashMap<String, Subscription> remoteNotifyCallbackId2InternalSub = new HashMap<String, Subscription>();
	@Value("${bootstrap.servers}")
	String BOOTSTRAP_SERVERS;

	HttpUtils httpUtils;

	// @Value("${notification.port}")
	// String REMOTE_NOTIFICATION_PORT;

	public SubscriptionService(SubscriptionManagerProducerChannel producerChannel) {
		this.producerChannel = producerChannel;
	}

	@PostConstruct
	private void setup() {

		httpUtils = HttpUtils.getInstance(contextResolverService);
		notificationHandler = new NotificationHandlerREST(this, contextResolverService);
		intervalHandler = new IntervalNotificationHandler(contextResolverService, notificationHandler,
				subscriptionId2Context);
		logger.trace("call loadStoredSubscriptions() ::");
		Map<String, byte[]> subs = kafkaOps.pullFromKafka(KafkaConstants.SUBSCRIPTIONS_TOPIC);
		for (byte[] sub : subs.values()) {
			try {
				if (Arrays.areEqual(sub, AppConstants.NULL_BYTES)) {
					continue;
				}
				SubscriptionRequest subscription = DataSerializer.getSubscriptionRequest(new String(sub));

				// if (subscription.getLdQuery() != null &&
				// !subscription.getLdQuery().trim().equals("")) {
				// subscription.setQueryTerm(queryParser.parseQuery(subscription.getLdQuery(),
				// contextResolverService.getContextAsSet(subscription.getId().toString())));
				// }
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

	@Override
	public URI subscribe(SubscriptionRequest subscriptionRequest) throws ResponseException {

		Subscription subscription = subscriptionRequest.getSubscription();
		if (subscription.getId() == null) {
			subscription.setId(generateUniqueSubId(subscription));
		} else {
			if (this.subscriptionId2Subscription.containsKey(subscription.getId().toString())) {
				throw new ResponseException(ErrorType.AlreadyExists);
			}
		}
		this.subscriptionId2Subscription.put(subscription.getId().toString(), subscription);
		this.subscriptionId2Context.putAll(subscription.getId().toString(), subscriptionRequest.getContext());
		this.sub2CreationTime.put(subscription, System.currentTimeMillis());
		for (EntityInfo info : subscription.getEntities()) {
			if (info.getId() != null) {
				idBasedSubscriptions.put(info.getId().toString(), subscription);
			} else if (info.getType() != null) {
				typeBasedSubscriptions.put(info.getType(), subscription);
			} else if (info.getIdPattern() != null) {
				idPatternBasedSubscriptions.put(info.getIdPattern(), subscription);
			}

		}
		syncToMessageBus(subscriptionRequest);

		if (subscription.getExpires() != null) {
			TimerTask cancel = new TimerTask() {

				@Override
				public void run() {
					synchronized (subscriptionId2Subscription) {
						subscriptionId2Subscription.remove(subscription.getId().toString());
					}
					synchronized (idBasedSubscriptions) {
						idBasedSubscriptions.values().remove(subscription);
					}
					synchronized (typeBasedSubscriptions) {
						typeBasedSubscriptions.values().remove(subscription);
					}
					synchronized (idPatternBasedSubscriptions) {
						idPatternBasedSubscriptions.values().remove(subscription);
					}

				}
			};
			subId2TimerTask.put(subscription.getId().toString(), cancel);
			watchDog.schedule(cancel, subscription.getExpires() - System.currentTimeMillis());
		}
		if (subscription.getTimeInterval() > 0) {
			intervalHandler.addSub(subscription.getId().toString(), subscription.getTimeInterval(),
					subscription.getNotification().getEndPoint().getUri(),
					subscription.getNotification().getEndPoint().getAccept());
		}
		return subscription.getId();
	}

	private void syncToMessageBus(SubscriptionRequest subscription) {
		if (!this.kafkaOps.isMessageExists(subscription.getSubscription().getId().toString(),
				KafkaConstants.SUBSCRIPTIONS_TOPIC)) {
			this.kafkaOps.pushToKafka(producerChannel.subscriptionWriteChannel(),
					subscription.getSubscription().getId().toString().getBytes(),
					DataSerializer.toJson(subscription).getBytes());
		}
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
		for (EntityInfo info : removedSub.getEntities()) {
			if (info.getId() != null) {
				synchronized (idBasedSubscriptions) {
					idBasedSubscriptions.remove(info.getId().toString(), removedSub);
				}

			} else if (info.getType() != null) {
				synchronized (typeBasedSubscriptions) {
					typeBasedSubscriptions.remove(info.getType(), removedSub);
				}

			} else if (info.getIdPattern() != null) {
				synchronized (idPatternBasedSubscriptions) {
					idPatternBasedSubscriptions.remove(info.getIdPattern(), removedSub);
				}

			}
			TimerTask task = subId2TimerTask.get(id.toString());
			if (task != null) {
				task.cancel();
			}
		}

		// TODO remove remote subscription

		this.kafkaOps.pushToKafka(this.producerChannel.subscriptionWriteChannel(), id.toString().getBytes(),
				"null".getBytes());

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
		String key = kafkaOps.getMessageKey(message);
		logger.debug("Create got called: " + payload);
		logger.debug(key);
		checkSubscriptionsWithCreate(key, payload, (long) message.getHeaders().get(KafkaHeaders.RECEIVED_TIMESTAMP));
	}

	private void checkSubscriptionsWithCreate(String key, String payload, long messageTime) {
		Entity create = DataSerializer.getEntity(payload);
		ArrayList<Subscription> subsToCheck = new ArrayList<Subscription>();
		subsToCheck.addAll(this.idBasedSubscriptions.get(key));
		subsToCheck.addAll(this.typeBasedSubscriptions.get(create.getType()));
		subsToCheck.addAll(getPatternBasedSubs(key));
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
		if (subscription.getTimeInterval() > 0) {
			try {
				intervalHandler.notify(new Notification(EntityTools.getRandomID("notification:"), new Date(),
						subscription.getId(), dataList, null, null, 0, true), subscription.getId().toString());
			} catch (URISyntaxException e) {
				logger.error("Exception ::", e);
				// Left empty intentionally
				throw new AssertionError();
			}
		} else {
			try {
				notificationHandler.notify(
						new Notification(EntityTools.getRandomID("notification:"), new Date(), subscription.getId(),
								dataList, null, null, 0, true),
						subscription.getNotification().getEndPoint().getUri(),
						subscription.getNotification().getEndPoint().getAccept(), subscription.getId().toString(),
						subscriptionId2Context.get(subscription.getId().toString()), subscription.getThrottling());
			} catch (URISyntaxException e) {
				logger.error("Exception ::", e);
				// Left empty intentionally
				throw new AssertionError();
			}
		}
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
		if(!shouldFire(entity, subscription)) {
			return result;
		}
		ArrayList<String> attribNames = getAttribNames(subscription);
		if (attribNames.isEmpty()) {
			return entity.getAllBaseProperties();
		}
		
		for (BaseProperty property : entity.getAllBaseProperties()) {
			if (attribNames.contains(property.getName())) {
				result.add(property);
			}
		}
		return result;
	}

	private boolean shouldFire(Entity entity, Subscription subscription) {
		if(subscription.getAttributeNames() == null || subscription.getAttributeNames().isEmpty()) {
			return true;
		}
		for(String attribName:subscription.getAttributeNames()) {
			for(BaseProperty baseProp: entity.getAllBaseProperties()) {
				if(attribName.equals(baseProp.getName())) {
					return true;
				}
			}
		}
		return false;
	}

	private Entity generateDataFromBaseOp(Entity deltaInfo, Subscription subscription) throws ResponseException {

		byte[] msg = kafkaOps.getMessage(deltaInfo.getId().toString(), KafkaConstants.ENTITY_TOPIC);
		Entity entity = DataSerializer.getEntity(new String(msg));
		if(!shouldFire(deltaInfo, subscription)) {
			return null;
		}
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
		Entity temp = new Entity(deltaInfo.getId(),entity.getType(), baseProps, entity.getRefToAccessControl());
		
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

		if (GEO_REL_EQUALS.equals(relation)) {
			if (location.getGeoValue() instanceof Point) {
				List<Double> geoValueAsList = java.util.Arrays.asList(((Point) location.getGeoValue()).lon(),
						((Point) location.getGeoValue()).lat());

				return geoValueAsList.equals(geoQuery.getCoordinates());
			} else {
				// TODO

				return false;
			}
		} else {

			Shape entityShape;
			if (location.getGeoValue() instanceof Point) {
				entityShape = shapeFactory.pointXY(((Point) location.getGeoValue()).lon(),
						((Point) location.getGeoValue()).lat());
			} else if (location.getGeoValue() instanceof Polygon) {
				PolygonBuilder polygonBuilder = shapeFactory.polygon();
				Iterator<SinglePosition> it = ((Polygon) location.getGeoValue()).positions().children().iterator()
						.next().children().iterator();
				while (it.hasNext()) {
					polygonBuilder.pointXY(((SinglePosition) it).coordinates().getLon(),
							((SinglePosition) it).coordinates().getLat());
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
				Shape bufferedShape = queryShape.getBuffered(geoQuery.getGeoRelation().getMaxDistance(),
						queryShape.getContext());
				if (geoQuery.getGeoRelation().getMaxDistance() != null) {
					return SpatialPredicate.IsWithin.evaluate(entityShape, bufferedShape);
				} else if (geoQuery.getGeoRelation().getMinDistance() != null) {
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

	// private Property getPropertyByName(String name, List<Property> properties) {
	// for (Property property : properties) {
	// if (property.getName().equals(name)) {
	// return property;
	// }
	// }
	// return null;
	// }

	private Collection<? extends Subscription> getPatternBasedSubs(String key) {
		ArrayList<Subscription> result = new ArrayList<Subscription>();
		for (String pattern : idPatternBasedSubscriptions.keySet()) {
			if (key.matches(pattern)) {
				result.addAll(idPatternBasedSubscriptions.get(pattern));
			}
		}
		return result;
	}

	@KafkaListener(topics = "${entity.update.topic}", groupId = "submanager")
	public void handleUpdate(Message<byte[]> message) {
		String payload = new String(message.getPayload());
		String key = kafkaOps.getMessageKey(message);
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
		subsToCheck.addAll(this.idBasedSubscriptions.get(key));
		subsToCheck.addAll(this.typeBasedSubscriptions.get(getTypeForId(key)));
		subsToCheck.addAll(getPatternBasedSubs(key));

		checkSubscriptions(subsToCheck, update, UPDATE, messageTime);

	}

	@KafkaListener(topics = "${entity.append.topic}", groupId = "submanager")
	public void handleAppend(Message<byte[]> message) {
		String payload = new String(message.getPayload());
		String key = kafkaOps.getMessageKey(message);
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
		subsToCheck.addAll(this.idBasedSubscriptions.get(key));
		subsToCheck.addAll(this.typeBasedSubscriptions.get(type));
		subsToCheck.addAll(getPatternBasedSubs(key));
		checkSubscriptions(subsToCheck, append, APPEND, messageTime);

	}

	// @StreamListener(SubscriptionManagerConsumerChannel.deleteReadChannel)
	@KafkaListener(topics = "${entity.delete.topic}", groupId = "submanager")
	public void handleDelete(Message<byte[]> message) throws Exception {
		// checkSubscriptionsWithDelete(new String((byte[])
		// message.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY)),
		// new String(message.getPayload()));
	}

	@KafkaListener(topics = "${csource.notification.topic}", groupId = "submanager")
	public void handleCSourceNotification(Message<byte[]> message) {
		String payload = new String(message.getPayload());
		String key = kafkaOps.getMessageKey(message);
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
		subsToCheck.addAll(this.idBasedSubscriptions.get(key));
		subsToCheck.addAll(this.typeBasedSubscriptions.get(delete.getType()));
		subsToCheck.addAll(getPatternBasedSubs(key));
		checkSubscriptions(subsToCheck, delete, DELETE, messageTime);

	}

	private String getTypeForId(String key) {
		byte[] json = kafkaOps.getMessage(key, KafkaConstants.ENTITY_TOPIC);
		if (json == null) {
			return "";
		}
		try {
			return objectMapper.readTree(json).get(JSON_LD_TYPE).get(0).asText("");
		} catch (IOException e) {
			logger.error("Exception ::", e);
			e.printStackTrace();
		}
		return "";
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
		this.subscriptionId2Subscription.get(subId).getNotification().setLastNotification(new Date(now));
		this.subscriptionId2Subscription.get(subId).getNotification().setLastSuccessfulNotification(new Date(now));

	}

	public void reportFailedNotification(String subId, Long now) {
		this.subscriptionId2Subscription.get(subId).getNotification().setLastFailedNotification(new Date(now));

	}

	public void reportSuccessfulNotification(String subId, Long now) {
		this.subscriptionId2Subscription.get(subId).getNotification().setLastSuccessfulNotification(new Date(now));

	}

}
