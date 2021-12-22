package eu.neclab.ngsildbroker.subscriptionmanager.service;

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
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.filosganga.geogson.model.Point;
import com.github.filosganga.geogson.model.Polygon;
import com.github.filosganga.geogson.model.positions.SinglePosition;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BaseProperty;
import eu.neclab.ngsildbroker.commons.datatypes.EndPoint;
import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.EntityRequest;
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
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import reactor.core.publisher.Mono;

@Service
public class SubscriptionService {
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
	ReplyingKafkaTemplate<String, String, String> kafkaTemplate;

	@Autowired
	@Qualifier("subwebclient")
	WebClient webClient;

	@Autowired
	SubscriptionInfoDAO subscriptionInfoDAO;



	@Value("${subscription.directdb:true}")
	boolean directDB;

	JtsShapeFactory shapeFactory = JtsSpatialContext.GEO.getShapeFactory();

	Table<String, String, SubscriptionRequest> tenant2subscriptionId2Subscription = HashBasedTable.create();
	Table<String, String, TimerTask> subId2TimerTask = HashBasedTable.create();
	Table<String, String, List<SubscriptionRequest>> type2EntitiesSubscriptions = HashBasedTable.create();
	HashMap<SubscriptionRequest, Long> sub2CreationTime = new HashMap<SubscriptionRequest, Long>();
	Table<String, String, List<Object>> tenantId2subscriptionId2Context = HashBasedTable.create();
	HashMap<String, SubscriptionRequest> remoteNotifyCallbackId2InternalSub = new HashMap<String, SubscriptionRequest>();
	@Value("${bootstrap.servers}")
	String BOOTSTRAP_SERVERS;

	private Table<String, String, String> tenant2Ids2Type;
	@Value("${subscriptions.topic:SUBSCRIPTIONS}")
	protected String subscriptionTopic;

	// @Value("${notification.port}")
	// String REMOTE_NOTIFICATION_PORT;

	@PostConstruct
	private void setup() {
		try {
			this.tenant2Ids2Type = subscriptionInfoDAO.getIds2Type();
		} catch (ResponseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		notificationHandlerREST = new NotificationHandlerREST(this, objectMapper, webClient);
		intervalHandlerREST = new IntervalNotificationHandler(notificationHandlerREST, kafkaTemplate, null,
				null);
		notificationHandlerMQTT = new NotificationHandlerMQTT(this, objectMapper);
		intervalHandlerMQTT = new IntervalNotificationHandler(notificationHandlerMQTT, kafkaTemplate, null,
				null);
		logger.trace("call loadStoredSubscriptions() ::");
		loadStoredSubscriptions();

	}

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

	public URI subscribe(SubscriptionRequest subscriptionRequest) throws ResponseException {
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
			storeSubscription(subscriptionRequest);

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

	private void storeSubscription(SubscriptionRequest subscription) throws ResponseException {
		kafkaTemplate.send(subscriptionTopic, subscription.getSubscription().getId().toString(),
				DataSerializer.toJson(subscription));

	}

	private URI generateUniqueSubId(Subscription subscription) {

		try {
			return new URI("urn:ngsi-ld:Subscription:" + subscription.hashCode());
		} catch (URISyntaxException e) {
			// Left empty intentionally should never happen
			throw new AssertionError();
		}
	}

	/*
	 * private void checkTenant(ArrayListMultimap<String, String> headers,
	 * ArrayListMultimap<String, String> headers2) throws ResponseException {
	 * List<String> tenant1 = headers.get(NGSIConstants.TENANT_HEADER); List<String>
	 * tenant2 = headers2.get(NGSIConstants.TENANT_HEADER); if (tenant1.size() !=
	 * tenant2.size()) { throw new ResponseException(ErrorType.NotFound); } if
	 * (tenant1.size() > 0 && !tenant1.get(0).equals(tenant2.get(0))) { throw new
	 * ResponseException(ErrorType.NotFound); }
	 * 
	 * }
	 */

	public void unsubscribe(URI id, ArrayListMultimap<String, String> headers) throws ResponseException {
		String tenant = HttpUtils.getInternalTenant(headers);
		SubscriptionRequest removedSub;
		synchronized (tenant2subscriptionId2Subscription) {
			if (!this.tenant2subscriptionId2Subscription.contains(tenant, id.toString())) {
				throw new ResponseException(ErrorType.NotFound, id.toString() + " not found");
			}
			removedSub = this.tenant2subscriptionId2Subscription.get(tenant, id.toString());
			if (removedSub == null) {
				throw new ResponseException(ErrorType.NotFound, id.toString() + " not found");
			}

			removedSub = this.tenant2subscriptionId2Subscription.remove(tenant, id.toString());
			kafkaTemplate.send(subscriptionTopic, id.toString(), "null");
		}

		synchronized (tenantId2subscriptionId2Context) {
			this.tenantId2subscriptionId2Context.remove(tenant, id.toString());
		}
		intervalHandlerREST.removeSub(id.toString());
		intervalHandlerMQTT.removeSub(id.toString());
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
		TimerTask task = subId2TimerTask.get(tenant, id.toString());
		if (task != null) {
			task.cancel();
		}
		// TODO remove remote subscription
	}

	public SubscriptionRequest updateSubscription(SubscriptionRequest subscriptionRequest) throws ResponseException {
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
			SubscriptionRequest result = new SubscriptionRequest(oldSub, subscriptionRequest.getContext(),
					subscriptionRequest.getHeaders());
			storeSubscription(result);
			return result;
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

	@KafkaListener(topics = "${entity.create.topic}")
	public void handleCreate(Message<String> message) {
		String key = (String) message.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY);
		logger.debug("Create got called: " + key);
		checkSubscriptionsWithCreate(DataSerializer.getEntityRequest(new String(message.getPayload())),
				(long) message.getHeaders().get(KafkaHeaders.RECEIVED_TIMESTAMP));
	}

	private void checkSubscriptionsWithCreate(EntityRequest createRequest, long messageTime) {
		Entity create = DataSerializer.getEntity(createRequest.getWithSysAttrs());
		String id = createRequest.getId();
		synchronized (this.tenant2Ids2Type) {
			this.tenant2Ids2Type.put(createRequest.getTenant(), id, create.getType());
		}

		ArrayList<SubscriptionRequest> subsToCheck = new ArrayList<SubscriptionRequest>();

		List<SubscriptionRequest> subs = this.type2EntitiesSubscriptions.get(createRequest.getTenant(),
				create.getType());
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

		checkSubscriptions(subsToCheck, create, CREATE, messageTime);

	}

	private void addAllTypeSubscriptions(ArrayListMultimap<String, String> entityOpHeaders,
			List<SubscriptionRequest> subsToCheck) {
		List<SubscriptionRequest> subs = this.type2EntitiesSubscriptions.get(ALL_TYPES_TYPE,
				HttpUtils.getInternalTenant(entityOpHeaders));
		if (subs == null) {
			return;
		}
		subsToCheck.addAll(subs);
	}

	private void checkSubscriptions(ArrayList<SubscriptionRequest> subsToCheck, Entity entity, int methodType,
			long messageTime) {

		for (SubscriptionRequest subscription : subsToCheck) {
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

	private void sendNotification(List<Entity> dataList, SubscriptionRequest subscription) {
		logger.debug(DataSerializer.toJson(dataList));
		try {
			String endpointProtocol = subscription.getSubscription().getNotification().getEndPoint().getUri()
					.getScheme();

			NotificationHandler handler;
			if (endpointProtocol.equals("mqtt")) {
				handler = notificationHandlerMQTT;
			} else {
				handler = notificationHandlerREST;
			}
			handler.notify(
					new Notification(EntityTools.getRandomID("notification:"), System.currentTimeMillis(),
							subscription.getSubscription().getId(), dataList, null, null, 0, true),
					subscription.getSubscription().getNotification().getEndPoint().getUri(),
					subscription.getSubscription().getNotification().getEndPoint().getAccept(),
					subscription.getSubscription().getId().toString(),
					tenantId2subscriptionId2Context.get(subscription.getTenant(),
							subscription.getSubscription().getId().toString()),
					subscription.getSubscription().getThrottling(),
					subscription.getSubscription().getNotification().getEndPoint().getNotifierInfo(),
					subscription.getTenant());
		} catch (URISyntaxException e) {
			logger.error("Exception ::", e);
			// Left empty intentionally
			throw new AssertionError();
		}
		// }
	}

	private Entity generateNotificationEntity(Entity entity, SubscriptionRequest subscription)
			throws ResponseException {

		if (!evaluateGeoQuery(subscription.getSubscription().getLdGeoQuery(), entity.getLocation())) {
			return null;
		}
		if (subscription.getSubscription().getQueryTerm() != null) {
			if (!subscription.getSubscription().getQueryTerm().calculate(entity.getAllBaseProperties())) {
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

	private List<BaseProperty> extractBaseProps(Entity entity, SubscriptionRequest subscription) {
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

	private boolean shouldFire(Entity entity, SubscriptionRequest subscription) {

		if (subscription.getSubscription().getAttributeNames() == null
				|| subscription.getSubscription().getAttributeNames().isEmpty()) {
			return true;
		}

		for (String attribName : subscription.getSubscription().getAttributeNames()) {
			for (BaseProperty baseProp : entity.getAllBaseProperties()) {
				if (attribName.equals(baseProp.getIdString())) {
					return true;
				}
			}
		}
		return false;
	}

	private Entity generateDataFromBaseOp(Entity deltaInfo, SubscriptionRequest subscription) throws ResponseException {
		String entityBody = null;
		if (!shouldFire(deltaInfo, subscription)) {
			return null;
		}

		if (directDB) {
			entityBody = subscriptionInfoDAO.getEntity(deltaInfo.getId().toString(), subscription.getTenant());
		}
		// HERE YOU NEED TO REPLACE THE ATTRIBUTE TO THE ONE FROM DELTA
		Entity entity = DataSerializer.getEntity(entityBody);
		if (!evaluateGeoQuery(subscription.getSubscription().getLdGeoQuery(), entity.getLocation())) {
			return null;
		}
		if (subscription.getSubscription().getQueryTerm() != null) {
			if (!subscription.getSubscription().getQueryTerm().calculate(entity.getAllBaseProperties())) {
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

	private ArrayList<String> getAttribNames(SubscriptionRequest subscription) {
		ArrayList<String> attribNames = new ArrayList<String>();
		if (subscription.getSubscription().getNotification().getAttributeNames() != null) {
			attribNames.addAll(subscription.getSubscription().getNotification().getAttributeNames());
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

	// private Property getPropertyByName(String name, List<Property> properties) {
	// for (Property property : properties) {
	// if (property.getName().equals(name)) {
	// return property;
	// }
	// }
	// return null;
	// }

	@KafkaListener(topics = "${entity.update.topic}")
	public void handleUpdate(Message<String> message) {
		String payload = new String(message.getPayload());
		String key = (String) message.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY);
		logger.debug("update got called: " + payload);
		logger.debug(key);
		EntityRequest updateRequest = DataSerializer.getEntityRequest(payload);
		checkSubscriptionsWithUpdate(updateRequest, (long) message.getHeaders().get(KafkaHeaders.RECEIVED_TIMESTAMP));
	}

	private void checkSubscriptionsWithUpdate(EntityRequest updateRequest, long messageTime) {
		Entity update = DataSerializer.getPartialEntity(updateRequest.getOperationValue());
		String id = updateRequest.getId();
		String type = getTypeForId(updateRequest.getTenant(), id);
		try {
			update.setId(new URI(id));
		} catch (URISyntaxException e) {

			e.printStackTrace();
		}
		update.setType(type);
		ArrayList<SubscriptionRequest> subsToCheck = new ArrayList<SubscriptionRequest>();

		List<SubscriptionRequest> subs = this.type2EntitiesSubscriptions.get(updateRequest.getTenant(), type);
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
		addAllTypeSubscriptions(updateRequest.getHeaders(), subsToCheck);
		checkSubscriptions(subsToCheck, update, UPDATE, messageTime);

	}

	@KafkaListener(topics = "${entity.append.topic}")
	public void handleAppend(Message<String> message) {
		String payload = new String(message.getPayload());
		String key = (String) message.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY);
		logger.debug("Append got called: " + payload);
		logger.debug(key);
		checkSubscriptionsWithAppend(DataSerializer.getEntityRequest(new String(message.getPayload())),
				(long) message.getHeaders().get(KafkaHeaders.RECEIVED_TIMESTAMP));
	}

	private void checkSubscriptionsWithAppend(EntityRequest appendRequest, long messageTime) {
		Entity append = DataSerializer.getPartialEntity(appendRequest.getOperationValue());
		String id = appendRequest.getId();
		String type = getTypeForId(appendRequest.getTenant(), id);

		try {
			append.setId(new URI(id));
		} catch (URISyntaxException e) {
			// left empty intentionally should never happen because the uri should be
			// already checked
			e.printStackTrace();
		}
		append.setType(type);

		ArrayList<SubscriptionRequest> subsToCheck = new ArrayList<SubscriptionRequest>();
		List<SubscriptionRequest> subs = this.type2EntitiesSubscriptions.get(appendRequest.getTenant(), type);
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
		checkSubscriptions(subsToCheck, append, APPEND, messageTime);

	}

	// @StreamListener(SubscriptionManagerConsumerChannel.deleteReadChannel)
	@KafkaListener(topics = "${entity.delete.topic}")
	public void handleDelete(Message<String> message) throws Exception {
		EntityRequest req = DataSerializer.getEntityRequest(new String(message.getPayload()));
		this.tenant2Ids2Type.remove(req.getTenant(), req.getId());
	}


	private void subscribeToRemote(SubscriptionRequest subscriptionRequest, ArrayList<String> remoteEndPoints) {
		new Thread() {
			@Override
			public void run() {

				Subscription remoteSub = new Subscription();
				Subscription subscription = subscriptionRequest.getSubscription();
				remoteSub.setCustomFlags(subscription.getCustomFlags());
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
						webClient.post().uri(temp.toString()).headers( httpHeadersOnWebClientBeingBuilt -> { 
					         httpHeadersOnWebClientBeingBuilt.addAll( additionalHeaders );
					    }).bodyValue(body)
					    .exchangeToMono(response -> {
					         if (response.statusCode().equals(HttpStatus.OK)) {
					             return Mono.just(Void.class);
					         }
					         else {
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

	@SuppressWarnings("unused")
	// Kept for now ... Delete notifications are not present
	private void checkSubscriptionsWithDelete(String key, String payload, long messageTime) {
		Entity delete = DataSerializer.getEntity(payload);
		ArrayList<SubscriptionRequest> subsToCheck = new ArrayList<SubscriptionRequest>();
		/*
		 * subsToCheck.addAll(this.idBasedSubscriptions.get(key));
		 * subsToCheck.addAll(this.typeBasedSubscriptions.get(delete.getType()));
		 */
		checkSubscriptions(subsToCheck, delete, DELETE, messageTime);

	}

	private String getTypeForId(String tenantId, String entityId) {
		synchronized (this.tenant2Ids2Type) {
			return this.tenant2Ids2Type.get(tenantId, entityId);
		}
		/*
		 * //this has to be db handled String json = kafkaOps.getMessage(key,
		 * KafkaConstants.ENTITY_TOPIC); if (json == null) { return ""; } try { return
		 * objectMapper.readTree(json).get(JSON_LD_TYPE).get(0).asText(""); } catch
		 * (IOException e) { logger.error("Exception ::", e); e.printStackTrace(); }
		 * return "";
		 */
	}

	public void remoteNotify(String id, Notification notification) {
		new Thread() {
			@Override
			public void run() {
				SubscriptionRequest subscription = remoteNotifyCallbackId2InternalSub.get(id);
				sendNotification(notification.getData(), subscription);
			}
		}.start();

	}

	public void reportNotification(String tenant, String subId, Long now) {
		SubscriptionRequest subscription;
		synchronized (tenant2subscriptionId2Subscription) {
			subscription = tenant2subscriptionId2Subscription.get(tenant, subId);
		}
		if (subscription != null) {
			subscription.getSubscription().getNotification().setLastNotification(new Date(now));
			subscription.getSubscription().getNotification().setLastSuccessfulNotification(new Date(now));
		}
	}

	public void reportFailedNotification(String tenant, String subId, Long now) {
		SubscriptionRequest subscription;
		synchronized (tenant2subscriptionId2Subscription) {
			subscription = tenant2subscriptionId2Subscription.get(tenant, subId);
		}
		if (subscription != null) {
			subscription.getSubscription().getNotification().setLastFailedNotification(new Date(now));
		}
	}

	public void reportSuccessfulNotification(String tenant, String subId, Long now) {
		synchronized (tenant2subscriptionId2Subscription) {
			SubscriptionRequest subscription = tenant2subscriptionId2Subscription.get(tenant, subId);
			if (subscription != null) {
				subscription.getSubscription().getNotification().setLastSuccessfulNotification(new Date(now));
			}
		}
	}

	// return true for future date validation
	private boolean isValidFutureDate(Long date) {
		return System.currentTimeMillis() < date;
	}
}
