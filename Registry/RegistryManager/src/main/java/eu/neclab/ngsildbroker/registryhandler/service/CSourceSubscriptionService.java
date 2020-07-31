package eu.neclab.ngsildbroker.registryhandler.service;

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
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.filosganga.geogson.model.Geometry;
import com.github.filosganga.geogson.model.Point;
import com.github.filosganga.geogson.model.Polygon;
import com.github.filosganga.geogson.model.positions.SinglePosition;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonParseException;
import com.netflix.discovery.EurekaClient;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.KafkaConstants;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceNotification;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.GeoRelation;
import eu.neclab.ngsildbroker.commons.datatypes.Information;
import eu.neclab.ngsildbroker.commons.datatypes.LDGeoQuery;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.TriggerReason;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.CSourceNotificationHandler;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.registryhandler.config.CSourceProducerChannel;

@Service
public class CSourceSubscriptionService {
	private final static Logger logger = LogManager.getLogger(CSourceSubscriptionService.class);

	private final byte[] nullArray = "null".getBytes();

	@Autowired
	@Qualifier("rmops")
	KafkaOps kafkaOps;

	@Autowired
	ObjectMapper objectMapper;

	@Autowired
	@Qualifier("rmconRes")
	ContextResolverBasic contextResolverService;

	@Autowired
	EurekaClient eurekaClient;

	@Autowired
	CSourceService cSourceService;

	CSourceNotificationHandler notificationHandler;
	CSourceNotificationHandler internalNotificationHandler;

	private final CSourceProducerChannel producerChannel;

	JtsShapeFactory shapeFactory = JtsSpatialContext.GEO.getShapeFactory();

	HashMap<URI, Subscription> subscriptionId2Subscription = new HashMap<URI, Subscription>();
	ArrayListMultimap<String, Subscription> idBasedSubscriptions = ArrayListMultimap.create();
	ArrayListMultimap<String, Subscription> typeBasedSubscriptions = ArrayListMultimap.create();
	ArrayListMultimap<String, Subscription> idPatternBasedSubscriptions = ArrayListMultimap.create();
	HashMap<String, Subscription> remoteNotifyCallbackId2InternalSub = new HashMap<String, Subscription>();
	// HashMap<String, Integer> subId2HashNotificationData = new HashMap<String,
	// Integer>();
	@Value("${bootstrap.servers}")
	String BOOTSTRAP_SERVERS;

	// @Value("${notification.port}")
	// String REMOTE_NOTIFICATION_PORT;

	/*
	 * Map<String, Object> props = new HashMap<String, Object>(); private
	 * SubscriptionManagerProducerChannel producerChannel; { // Make configurable
	 * props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
	 * props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
	 * ByteArrayDeserializer.class);
	 * props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
	 * ByteArrayDeserializer.class); props.put(ConsumerConfig.GROUP_ID_CONFIG,
	 * UUID.randomUUID().toString()); props.put(JsonDeserializer.TRUSTED_PACKAGES,
	 * "*"); }
	 */

	public CSourceSubscriptionService(CSourceProducerChannel producerChannel) {
		this.producerChannel = producerChannel;
		// loadStoredSubscriptions();
	}

	@PostConstruct
	private void loadStoredSubscriptions() {
		// this.contextResolver =
		// ContextResolverService.getInstance(producerChannel.atContextWriteChannel(),
		// kafkaOps);
		this.notificationHandler = new CSourceNotificationHandlerREST(contextResolverService);
		this.internalNotificationHandler = new CSourceNotificationHandlerInternalKafka(kafkaOps, producerChannel);
		logger.trace("call loadStoredSubscriptions() ::");
		Map<String, byte[]> subs = kafkaOps.pullFromKafka(KafkaConstants.CSOURCE_SUBSCRIPTIONS_TOPIC);
		for (byte[] sub : subs.values()) {
			try {
				if (Arrays.areEqual(sub, nullArray)) {
					continue;
				}
				SubscriptionRequest subscriptionRequest = DataSerializer.getSubscriptionRequest(new String(sub));
				subscribe(subscriptionRequest, false);
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
		subs = kafkaOps.pullFromKafka(KafkaConstants.SUBSCRIPTIONS_TOPIC);
		for (byte[] sub : subs.values()) {
			try {
				if (Arrays.areEqual(sub, nullArray)) {
					continue;
				}
				SubscriptionRequest subscriptionRequest = DataSerializer.getSubscriptionRequest(new String(sub));
				subscriptionRequest.getSubscription().setInternal(true);
				subscribe(subscriptionRequest, false);
			} catch (JsonParseException e) {
				// logger.error("Exception ::", e);
				// e.printStackTrace();
				continue;
			} catch (ResponseException e) {
				// logger.error("Exception ::", e);
				// e.printStackTrace();
				continue;
			}
		}

	}

	public Subscription querySubscription(URI id) throws ResponseException {
		if (subscriptionId2Subscription.containsKey(id)) {
			return subscriptionId2Subscription.get(id);
		} else {
			throw new ResponseException(ErrorType.NotFound);
		}

	}

	public URI subscribe(SubscriptionRequest subscriptionRequest) throws ResponseException {
		return subscribe(subscriptionRequest, true);
	}

	public URI subscribe(SubscriptionRequest subscriptionRequest, boolean sync) throws ResponseException {
		Subscription subscription = subscriptionRequest.getSubscription();
		if (subscription.getId() == null) {
			subscription.setId(generateUniqueSubId(subscription));
		} else {
			if (this.subscriptionId2Subscription.containsKey(subscription.getId())) {
				throw new ResponseException(ErrorType.AlreadyExists);
			}
		}
		this.subscriptionId2Subscription.put(subscription.getId(), subscription);
		for (EntityInfo info : subscription.getEntities()) {
			if (info.getId() != null) {
				idBasedSubscriptions.put(info.getId().toString(), subscription);
			} else if (info.getType() != null) {
				typeBasedSubscriptions.put(info.getType(), subscription);
			} else if (info.getIdPattern() != null) {
				idPatternBasedSubscriptions.put(info.getIdPattern(), subscription);
			}

		}
		if (sync) {
			syncToMessageBus(subscriptionRequest);
		}
		if (subscription.isInternal()) {
			new Thread() {
				public void run() {
					generateInitialNotification(subscription);
				}
			}.start();
		}
		return subscription.getId();
	}

	private void generateInitialNotification(Subscription subscription) {
		List<JsonNode> registrations;

		try {
			registrations = cSourceService.getCSourceRegistrations();

			for (JsonNode reg : registrations) {
				CSourceRegistration regEntry = DataSerializer
						.getCSourceRegistration(objectMapper.writeValueAsString(reg));
				if (!regEntry.isInternal()) {
					CSourceNotification notifyEntry = generateNotificationEntry(regEntry, subscription,
							TriggerReason.newlyMatching);
					if (notifyEntry != null) {
						internalNotificationHandler.notify(notifyEntry, subscription);
					}
				}
			}
		} catch (Exception e) {
			logger.error("Failed to get initial notification from registry");
			logger.error(e);
		}

	}

	private void syncToMessageBus(SubscriptionRequest subscriptionRequest) throws ResponseException {
		String id = subscriptionRequest.getSubscription().getId().toString();
		if (!this.kafkaOps.isMessageExists(id, KafkaConstants.CSOURCE_SUBSCRIPTIONS_TOPIC)) {
			this.kafkaOps.pushToKafka(producerChannel.csourceSubscriptionWriteChannel(), id.getBytes(),
					DataSerializer.toJson(subscriptionRequest).getBytes());
		}
	}

	private URI generateUniqueSubId(Subscription subscription) {

		try {
			return new URI("urn:ngsi-ld:CSourceSubscription:" + subscription.hashCode());
		} catch (URISyntaxException e) {
			logger.error("Exception ::", e);
			// Left empty intentionally should never happen
			throw new AssertionError();
		}
	}

	public boolean unsubscribe(URI id) throws ResponseException {
		Subscription removedSub = this.subscriptionId2Subscription.remove(id);
		if (removedSub == null) {
			throw new ResponseException(ErrorType.NotFound);
		}
		for (EntityInfo info : removedSub.getEntities()) {
			if (info.getId() != null) {
				idBasedSubscriptions.remove(info.getId().toString(), removedSub);
			} else if (info.getType() != null) {
				typeBasedSubscriptions.remove(info.getType(), removedSub);
			} else if (info.getIdPattern() != null) {
				idPatternBasedSubscriptions.remove(info.getIdPattern(), removedSub);
			}
		}

		this.kafkaOps.pushToKafka(this.producerChannel.csourceSubscriptionWriteChannel(), id.toString().getBytes(),
				"null".getBytes());
		return true;
	}

	public Subscription updateSubscription(Subscription subscription) throws ResponseException {
		Subscription oldSub = subscriptionId2Subscription.get(subscription.getId());
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
		return oldSub;
	}

	public List<Subscription> getAllSubscriptions(int limit) {
		List<Subscription> result = new ArrayList<Subscription>();
		for (Subscription sub : subscriptionId2Subscription.values()) {
			if (!sub.isInternal()) {
				result.add(sub);
			}
		}
		if (limit > 0) {
			if (limit < result.size()) {
				result = result.subList(0, limit);
			}
		}
		return result;
	}

	public Subscription getSubscription(URI subscriptionId) throws ResponseException {
		if (subscriptionId2Subscription.containsKey(subscriptionId)) {
			return subscriptionId2Subscription.get(subscriptionId);
		} else {
			throw new ResponseException(ErrorType.NotFound);
		}

	}

	public void checkSubscriptions(CSourceRegistration cSourceRegistration, TriggerReason triggerReason) {
		new Thread() {
			@Override
			public void run() {
				HashSet<Subscription> subsToCheck = new HashSet<Subscription>();
				for (Information info : cSourceRegistration.getInformation()) {
					for (EntityInfo entityInfo : info.getEntities()) {

						if (entityInfo.getId() != null) {
							subsToCheck.addAll(idBasedSubscriptions.get(entityInfo.getId().toString()));
						}
						subsToCheck.addAll(typeBasedSubscriptions.get(entityInfo.getType()));
						if (entityInfo.getId() != null) {
							subsToCheck.addAll(getPatternBasedSubs(entityInfo.getId().toString()));
						}
						if (entityInfo.getIdPattern() != null) {
							subsToCheck.addAll(getSubsForIdPattern(entityInfo.getIdPattern()));
						}

					}

				}
				for (Subscription sub : subsToCheck) {
					CSourceNotification notifyEntry = generateNotificationEntry(cSourceRegistration, sub,
							triggerReason);
					if (notifyEntry != null) {
						new Thread() {
							@Override
							public void run() {
								if (sub.isInternal()) {
									internalNotificationHandler.notify(notifyEntry, sub);
								} else {
									notificationHandler.notify(notifyEntry, sub);
								}

							}
						}.start();
					}

				}

			}
		}.start();

	}

	private Collection<? extends Subscription> getSubsForIdPattern(String idPattern) {
		ArrayList<Subscription> result = new ArrayList<Subscription>();
		for (String pattern : idPatternBasedSubscriptions.keySet()) {
			if (idPattern.matches(pattern)) {
				result.addAll(idPatternBasedSubscriptions.get(pattern));
			}
		}

		for (String id : idBasedSubscriptions.keySet()) {
			if (id.matches(idPattern)) {
				result.addAll(idBasedSubscriptions.get(id));
			}
		}
		return result;
	}

	private CSourceNotification generateNotificationEntry(CSourceRegistration regEntry, Subscription subscription,
			TriggerReason triggerReason) {
		if (subscription.getLdGeoQuery() != null && regEntry.getLocation() != null) {
			if (subscription.isInternal()) {
				if (!evaluateRegGeoQuery(subscription.getLdGeoQuery(), regEntry.getLocation())) {
					return null;
				}
			} else {
				if (!evaluateGeoQuery(subscription.getLdGeoQuery(), regEntry.getLocation())) {
					return null;
				}
			}
		}

		CSourceRegistration reg = new CSourceRegistration();
		reg.setDescription(regEntry.getDescription());
		reg.setName(regEntry.getName());
		reg.setEndpoint(regEntry.getEndpoint());
		reg.setExpires(regEntry.getExpires());
		reg.setId(regEntry.getId());
		reg.setLocation(regEntry.getLocation());
		reg.setTimestamp(regEntry.getTimestamp());
		reg.setType(regEntry.getType());
		ArrayList<Information> temp = new ArrayList<Information>();
		for (Information info : regEntry.getInformation()) {

			Information newInfo = new Information();
			temp.add(newInfo);
			ArrayList<EntityInfo> newEntityInfos = new ArrayList<>();
			newInfo.setEntities(newEntityInfos);
			for (EntityInfo regEntityInfo : info.getEntities()) {
				for (EntityInfo subEntityInfo : subscription.getEntities()) {
					if (!subEntityInfo.getType().equals(regEntityInfo.getType())) {
						continue;
					}
					if (subEntityInfo.getId() != null) {
						// id match
						if ((regEntityInfo.getId() != null && subEntityInfo.getId().equals(regEntityInfo.getId()))
								|| (regEntityInfo.getIdPattern() != null
										&& subEntityInfo.getId().toString().matches(regEntityInfo.getIdPattern()))) {
							addAttribMatch(info, regEntityInfo, subscription, newEntityInfos, newInfo);
						}

					} else if (subEntityInfo.getIdPattern() != null && ((regEntityInfo.getId() != null
							&& regEntityInfo.getId().toString().matches(subEntityInfo.getIdPattern()))
							|| (regEntityInfo.getIdPattern() != null
									&& regEntityInfo.getIdPattern().matches(subEntityInfo.getIdPattern())))) {
						// regex match
						addAttribMatch(info, regEntityInfo, subscription, newEntityInfos, newInfo);
					} else {
						// type match
						addAttribMatch(info, regEntityInfo, subscription, newEntityInfos, newInfo);
					}
				}

			}
		}
		reg.setInformation(temp);
		ArrayList<CSourceRegistration> data = new ArrayList<CSourceRegistration>();
		data.add(reg);
		try {
			return new CSourceNotification(EntityTools.getRandomID("csourcenotify"), subscription.getId(),
					new Date(System.currentTimeMillis()), triggerReason, data, null, null, -1, true);
		} catch (URISyntaxException e) {
			// Left empty intentionally should never happen
			throw new AssertionError();
		}
	}

	private void addAttribMatch(Information info, EntityInfo regEntityInfo, Subscription subscription,
			ArrayList<EntityInfo> newEntityInfos, Information newInfo) {
		Set<String> props = extractProperties(info, subscription);
		Set<String> relations = extractRelationShips(info, subscription);
		if (props != null || relations != null) {
			newEntityInfos.add(regEntityInfo);
			if (props != null) {
				newInfo.getProperties().addAll(props);
			}
			if (relations != null) {
				newInfo.getRelationships().addAll(props);
			}
		}

	}

	private Set<String> extractRelationShips(Information info, Subscription subscription) {
		if (info.getRelationships() == null || info.getRelationships().isEmpty()
				|| subscription.getAttributeNames() == null || subscription.getAttributeNames().isEmpty()) {
			return new HashSet<String>();
		}
		HashSet<String> result = new HashSet<String>();
		HashSet<String> attribNames = new HashSet<String>();
		attribNames.addAll(subscription.getNotification().getAttributeNames());
		attribNames.addAll(subscription.getAttributeNames());
		for (String relationship : info.getRelationships()) {
			if (attribNames.contains(relationship)) {
				result.add(relationship);
			}
		}
		if (result.isEmpty()) {
			return null;
		}
		return result;
	}

	private Set<String> extractProperties(Information info, Subscription subscription) {
		if (info.getProperties() == null || info.getProperties().isEmpty() || subscription.getAttributeNames() == null
				|| subscription.getAttributeNames().isEmpty()) {
			return new HashSet<String>();
		}
		HashSet<String> attribNames = new HashSet<String>();
		attribNames.addAll(subscription.getNotification().getAttributeNames());
		attribNames.addAll(subscription.getAttributeNames());
		HashSet<String> result = new HashSet<String>();
		for (String property : info.getProperties()) {
			if (attribNames.contains(property)) {
				result.add(property);
			}
		}
		if (result.isEmpty()) {
			return null;
		}
		return result;
	}

	private boolean evaluateGeoQuery(LDGeoQuery geoQuery, Geometry<?> location) {
		return evaluateGeoQuery(geoQuery, location, -1);
	}

	private boolean evaluateGeoQuery(LDGeoQuery geoQuery, Geometry<?> location, double expandArea) {

		if (geoQuery == null) {
			return true;
		}

		String relation = geoQuery.getGeoRelation().getRelation();
		List<Double> coordinates = geoQuery.getCoordinates();

		if (location == null) {
			return false;
		}

		if (GEO_REL_EQUALS.equals(relation)) {
			if (location instanceof Point) {
				List<Double> geoValueAsList = java.util.Arrays.asList(((Point) location).lon(),
						((Point) location).lat());
				return geoValueAsList.equals(geoQuery.getCoordinates());
			} else {

				return false;
			}
		} else {
			Shape entityShape;
			if (location instanceof Point) {
				entityShape = shapeFactory.pointXY(((Point) location).lon(), ((Point) location).lat());
			} else if (location instanceof Polygon) {
				PolygonBuilder polygonBuilder = shapeFactory.polygon();
				Iterator<SinglePosition> it = ((Polygon) location).positions().children().iterator().next().children()
						.iterator();
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
				Shape bufferedShape = queryShape.getBuffered(geoQuery.getGeoRelation().getMaxDistanceAsDouble(),
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

	private Collection<? extends Subscription> getPatternBasedSubs(String key) {
		ArrayList<Subscription> result = new ArrayList<Subscription>();
		for (String pattern : idPatternBasedSubscriptions.keySet()) {
			if (key.matches(pattern)) {
				result.addAll(idPatternBasedSubscriptions.get(pattern));
			}
		}
		return result;
	}

	@KafkaListener(topics = "${submanager.subscription.topic}", groupId = "csourcemanager")
	public void handleInternalSub(Message<byte[]> message) {
		if (Arrays.areEqual(AppConstants.NULL_BYTES, message.getPayload())) {
			try {
				unsubscribe(new URI(kafkaOps.getMessageKey(message)));
			} catch (ResponseException e) {
				logger.error(e);
			} catch (URISyntaxException e) {
				logger.error(e);
			}
		} else {
			SubscriptionRequest internalSub = DataSerializer.getSubscriptionRequest(new String(message.getPayload()));
			internalSub.getSubscription().setInternal(true);
			try {
				subscribe(internalSub);
			} catch (ResponseException e) {
				logger.error(e);
			}
		}
	}

	private boolean evaluateRegGeoQuery(LDGeoQuery subGeoQuery, Geometry<?> geoValue) {

		LDGeoQuery regGeoQuery = new LDGeoQuery();
		regGeoQuery.setCoordinates(subGeoQuery.getCoordinates());
		regGeoQuery.setGeometry(subGeoQuery.getGeometry());
		regGeoQuery.setGeoProperty(subGeoQuery.getGeoProperty());
		GeoRelation newRel = new GeoRelation();
		regGeoQuery.setGeoRelation(newRel);
		GeoRelation origRel = subGeoQuery.getGeoRelation();
		String origRelString = origRel.getRelation();
		if (origRelString.equals(GEO_REL_NEAR)) {
			if (origRel.getMinDistance() != null) {
				newRel.setRelation(GEO_REL_WITHIN);
				return !evaluateGeoQuery(regGeoQuery, geoValue, origRel.getMinDistanceAsDouble());
			} else if (origRel.getMaxDistance() != null) {
				newRel.setRelation(GEO_REL_INTERSECTS);
				return evaluateGeoQuery(regGeoQuery, geoValue, origRel.getMaxDistanceAsDouble());
			}
		} else if (origRelString.equals(GEO_REL_CONTAINS)) {
			newRel.setRelation(origRelString);
			return evaluateGeoQuery(regGeoQuery, geoValue);
		} else if (origRelString.equals(GEO_REL_WITHIN)) {
			newRel.setRelation(GEO_REL_CONTAINS);
			return evaluateGeoQuery(regGeoQuery, geoValue);
		} else if (origRelString.equals(GEO_REL_INTERSECTS)) {
			newRel.setRelation(origRelString);
			return evaluateGeoQuery(regGeoQuery, geoValue);
		} else if (origRelString.equals(GEO_REL_EQUALS)) {
			newRel.setRelation(GEO_REL_CONTAINS);
			return evaluateGeoQuery(regGeoQuery, geoValue);
		} else if (origRelString.equals(GEO_REL_DISJOINT)) {
			newRel.setRelation(GEO_REL_WITHIN);
			return !evaluateGeoQuery(regGeoQuery, geoValue);
		} else if (origRelString.equals(GEO_REL_OVERLAPS)) {
			newRel.setRelation(GEO_REL_CONTAINS);
			if (evaluateGeoQuery(regGeoQuery, geoValue)) {
				return true;
			}
			newRel.setRelation(GEO_REL_WITHIN);
			return evaluateGeoQuery(regGeoQuery, geoValue);

		}

		return false;

	}

	// TODO this is potentially slow as hell so figure out a better way to do
	// this!!!
	public boolean checkSubscriptions(CSourceRegistration prevCSourceRegistration,
			CSourceRegistration newCSourceRegistration) {
		new Thread() {
			@Override
			public void run() {
				HashMap<Subscription, CSourceNotification> oldNotification = new HashMap<Subscription, CSourceNotification>();
				HashMap<Subscription, CSourceNotification> newNotification = new HashMap<Subscription, CSourceNotification>();

				HashSet<Subscription> prevSubsToCheck = new HashSet<Subscription>();
				for (Information info : prevCSourceRegistration.getInformation()) {
					for (EntityInfo entityInfo : info.getEntities()) {

						if (entityInfo.getId() != null) {
							prevSubsToCheck.addAll(idBasedSubscriptions.get(entityInfo.getId().toString()));
						}
						prevSubsToCheck.addAll(typeBasedSubscriptions.get(entityInfo.getType()));
						prevSubsToCheck.addAll(getPatternBasedSubs(entityInfo.getId().toString()));
						if (entityInfo.getIdPattern() != null) {
							prevSubsToCheck.addAll(getSubsForIdPattern(entityInfo.getIdPattern()));
						}

					}

				}

				HashSet<Subscription> newSubsToCheck = new HashSet<Subscription>();
				for (Information info : prevCSourceRegistration.getInformation()) {
					for (EntityInfo entityInfo : info.getEntities()) {

						if (entityInfo.getId() != null) {
							newSubsToCheck.addAll(idBasedSubscriptions.get(entityInfo.getId().toString()));
						}
						newSubsToCheck.addAll(typeBasedSubscriptions.get(entityInfo.getType()));
						newSubsToCheck.addAll(getPatternBasedSubs(entityInfo.getId().toString()));
						if (entityInfo.getIdPattern() != null) {
							newSubsToCheck.addAll(getSubsForIdPattern(entityInfo.getIdPattern()));
						}

					}

				}

				for (Subscription sub : prevSubsToCheck) {

					CSourceNotification notifyEntry = generateNotificationEntry(prevCSourceRegistration, sub, null);
					if (notifyEntry != null) {
						oldNotification.put(sub, notifyEntry);
					}
				}
				for (Subscription sub : newSubsToCheck) {

					CSourceNotification notifyEntry = generateNotificationEntry(newCSourceRegistration, sub, null);
					if (notifyEntry != null) {
						newNotification.put(sub, notifyEntry);
					}
				}
				TriggerReason trigger;
				for (Entry<Subscription, CSourceNotification> entry : newNotification.entrySet()) {
					Subscription sub = entry.getKey();
					if (oldNotification.containsKey(sub)) {
						if (oldNotification.hashCode() == entry.getValue().hashCode()) {
							// no changes for sub -> no notification
							continue;
						}
						// updated notification
						trigger = TriggerReason.updated;
					} else {
						// new notification
						trigger = TriggerReason.newlyMatching;
					}
					entry.getValue().setTriggerReason(trigger);
					new Thread() {
						@Override
						public void run() {
							if (sub.isInternal()) {
								internalNotificationHandler.notify(entry.getValue(), sub);
							} else {
								notificationHandler.notify(entry.getValue(), sub);
							}

						}
					}.start();
				}

				for (Entry<Subscription, CSourceNotification> entry : oldNotification.entrySet()) {
					if (!newNotification.containsKey(entry.getKey())) {
						// deleted notification
						CSourceNotification deleteNotification = new CSourceNotification(entry.getValue().getId(),
								entry.getValue().getSubscriptionId(), new Date(System.currentTimeMillis()),
								TriggerReason.noLongerMatching, null, null, null, 0, true);
						new Thread() {
							@Override
							public void run() {
								if (entry.getKey().isInternal()) {
									internalNotificationHandler.notify(deleteNotification, entry.getKey());
								} else {
									notificationHandler.notify(deleteNotification, entry.getKey());
								}

							}
						}.start();
					}
				}
			}
		}.start();
		return true;
	}

}
