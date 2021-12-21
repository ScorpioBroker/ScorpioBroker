package eu.neclab.ngsildbroker.entityhandler.services;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLTransientConnectionException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.github.filosganga.geogson.model.Geometry;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.AppendEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.AppendResult;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.CreateCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.DeleteEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.GeoProperty;
import eu.neclab.ngsildbroker.commons.datatypes.Information;
import eu.neclab.ngsildbroker.commons.datatypes.Property;
import eu.neclab.ngsildbroker.commons.datatypes.Relationship;
import eu.neclab.ngsildbroker.commons.datatypes.TimeInterval;
import eu.neclab.ngsildbroker.commons.datatypes.UpdateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntityCRUDService;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.storage.StorageWriterDAO;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;

@Service
@EnableAutoConfiguration
@EnableKafka
public class EntityService implements EntityCRUDService {

	@Value("${entity.topic}")
	String ENTITY_TOPIC;
	@Value("${entity.create.topic}")
	String ENTITY_CREATE_TOPIC;
	@Value("${entity.append.topic}")
	String ENTITY_APPEND_TOPIC;
	@Value("${entity.update.topic}")
	String ENTITY_UPDATE_TOPIC;
	@Value("${entity.delete.topic}")
	String ENTITY_DELETE_TOPIC;
	@Value("${csources.registration.topic:CONTEXT_REGISTRY}")
	String CSOURCE_TOPIC;
	@Value("${bootstrap.servers}")
	String bootstrapServers;
	@Value("${append.overwrite}")
	String appendOverwriteFlag;
	@Value("${entity.index.topic}")
	String ENTITY_INDEX;

	@Value("${batchoperations.maxnumber.create:-1}")
	int maxCreateBatch;
	@Value("${batchoperations.maxnumber.update:-1}")
	int maxUpdateBatch;
	@Value("${batchoperations.maxnumber.upsert:-1}")
	int maxUpsertBatch;
	@Value("${batchoperations.maxnumber.delete:-1}")
	int maxDeleteBatch;

	boolean directDB = true;
	public static boolean checkEntity = false;
	@Autowired
	@Qualifier("emdao")
	StorageWriterDAO storageWriterDao;

	@Autowired
	EntityInfoDAO entityInfoDAO;

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	/*
	 * @Autowired
	 * 
	 * @Qualifier("emtopicmap") EntityTopicMap entityTopicMap;
	 */

	LocalDateTime startAt;
	LocalDateTime endAt;
	private ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
	private final static Logger logger = LogManager.getLogger(EntityService.class);

	// construct in-memory
	@PostConstruct
	private void loadStoredEntitiesDetails() throws ResponseException {
		synchronized (this.entityIds) {
			this.entityIds = entityInfoDAO.getAllIds();
		}
		logger.trace("filling in-memory hashmap completed:");
	}

	/**
	 * Method to publish jsonld message to kafka topic
	 * 
	 * @param resolved jsonld message
	 * @param headers
	 * @return RestResponse
	 * @throws KafkaWriteException,Exception
	 * @throws ResponseException
	 */
	public String createMessage(ArrayListMultimap<String, String> headers, Map<String, Object> resolved)
			throws ResponseException, Exception {
		// get message channel for ENTITY_CREATE topic.
		logger.debug("createMessage() :: started");
		// MessageChannel messageChannel = producerChannels.createWriteChannel();
		EntityRequest request = new CreateEntityRequest(resolved, headers);

		String tenantId = HttpUtils.getInternalTenant(headers);

		synchronized (this.entityIds) {
			if (this.entityIds.containsEntry(tenantId, request.getId())) {
				throw new ResponseException(ErrorType.AlreadyExists, request.getId() + " already exists");
			}
			this.entityIds.put(tenantId, request.getId());
		}
		pushToDB(request);
		new Thread() {
			public void run() {
				try {
					registerContext(request);
					kafkaTemplate.send(ENTITY_CREATE_TOPIC, request.getId(), DataSerializer.toJson(request));
				} catch (URISyntaxException | IOException | ResponseException e) {
					logger.error(e);
				}
			};
		}.start();

		logger.debug("createMessage() :: completed");
		return request.getId();
	}

	private void pushToDB(EntityRequest request) {
		boolean success = false;
		while (!success) {
			try {
				logger.debug("Received message: " + request.getWithSysAttrs());
				logger.trace("Writing data...");
				if (storageWriterDao != null && storageWriterDao.storeEntity(request)) {

					logger.trace("Writing is complete");
				}
				success = true;
			} catch (SQLTransientConnectionException e) {
				logger.warn("SQL Exception attempting retry");
				Random random = new Random();
				int randomNumber = random.nextInt(4000) + 500;
				try {
					Thread.sleep(randomNumber);
				} catch (InterruptedException e1) {
					logger.error(e1);
				}
			}
		}
	}

	/**
	 * Method to update a existing Entity in the system/kafka topic
	 * 
	 * @param entityId - id of entity to be updated
	 * @param resolved - jsonld message containing fileds to be updated with updated
	 *                 values
	 * @return RestResponse
	 * @throws ResponseException
	 * @throws IOException
	 */
	public UpdateResult updateMessage(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> resolved) throws ResponseException, Exception {
		logger.trace("updateMessage() :: started");
		// get message channel for ENTITY_UPDATE topic

		String tenantid = HttpUtils.getInternalTenant(headers);
		// get entity details
		Map<String, Object> entityBody = validateIdAndGetBody(entityId, tenantid);
		// String entityBody = validateIdAndGetBody(entityId);
		UpdateEntityRequest request = new UpdateEntityRequest(headers, entityId, entityBody, resolved, null);

		// update fields

		// pubilsh merged message
		// & check if anything is changed.
		if (request.getStatus()) {
			if (directDB) {
				pushToDB(request);
			}
			new Thread() {
				public void run() {
					kafkaTemplate.send(ENTITY_UPDATE_TOPIC, entityId, DataSerializer.toJson(request));
					updateContext(request);
				};
			}.start();
		}
		logger.trace("updateMessage() :: completed");
		return request.getUpdateResult();
	}

	/**
	 * Method to append fields in existing Entity in system/kafka topic
	 * 
	 * @param entityId - id of entity to be appended
	 * @param resolved - jsonld message containing fileds to be appended
	 * @return AppendResult
	 * @throws ResponseException
	 * @throws IOException
	 */
	public AppendResult appendMessage(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> resolved, String overwriteOption) throws ResponseException, Exception {
		logger.trace("appendMessage() :: started");
		// get message channel for ENTITY_APPEND topic
		// payload validation
		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData, "empty entity id is not allowed");
		}

		String tenantId = HttpUtils.getInternalTenant(headers);
		// get entity details
		Map<String, Object> entityBody = validateIdAndGetBody(entityId, tenantId);
		AppendEntityRequest request = new AppendEntityRequest(headers, entityId, entityBody, resolved, overwriteOption,
				this.appendOverwriteFlag);
		// get entity from ENTITY topic.
		// pubilsh merged message
		// check if anything is changed

		if (directDB) {
			pushToDB(request);
		}
		new Thread() {
			public void run() {
				kafkaTemplate.send(ENTITY_APPEND_TOPIC, entityId, DataSerializer.toJson(request));
			};
		}.start();

		logger.trace("appendMessage() :: completed");
		return request.getAppendResult();
	}

	private Map<String, Object> validateIdAndGetBody(String entityId, String tenantId) throws ResponseException {
		// null id check
		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed");
		}
		synchronized (this.entityIds) {
			if (!this.entityIds.containsKey(tenantId)) {
				throw new ResponseException(ErrorType.TenantNotFound, "tenant " + tenantId + " not found");
			}
			if (!this.entityIds.containsValue(entityId)) {
				throw new ResponseException(ErrorType.NotFound, "Entity Id " + entityId + " not found");
			}
		}
		String entityBody = null;
		if (directDB) {
			entityBody = this.entityInfoDAO.getEntity(entityId, tenantId);
		} else {
			// todo add back storage manager calls
		}

		try {
			return (Map<String, Object>) JsonUtils.fromString(entityBody);
		} catch (IOException e) {
			throw new AssertionError("can't load internal json");
		}
	}

	public boolean deleteEntity(ArrayListMultimap<String, String> headers, String entityId)
			throws ResponseException, Exception {
		logger.trace("deleteEntity() :: started");
		// get message channel for ENTITY_DELETE topic
		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed");
		}
		String tenantId = HttpUtils.getInternalTenant(headers);
		synchronized (this.entityIds) {

			if (!this.entityIds.containsEntry(tenantId, entityId)) {
				throw new ResponseException(ErrorType.NotFound, entityId + " not found");
			}
			this.entityIds.remove(tenantId, entityId);

		}
		EntityRequest request = new DeleteEntityRequest(entityId, headers);
		if (directDB) {
			pushToDB(request);
		}
		new Thread() {
			public void run() {
				kafkaTemplate.send(ENTITY_DELETE_TOPIC, entityId, DataSerializer.toJson(request));
			};
		}.start();
		logger.trace("deleteEntity() :: completed");
		return true;
	}

	public UpdateResult partialUpdateEntity(ArrayListMultimap<String, String> headers, String entityId, String attrId,
			Map<String, Object> expandedPayload) throws ResponseException, Exception {
		logger.trace("partialUpdateEntity() :: started");
		// get message channel for ENTITY_APPEND topic
		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed");
		}

		String tenantid = HttpUtils.getInternalTenant(headers);

		// get entity details
		Map<String, Object> entityBody = validateIdAndGetBody(entityId, tenantid);

		// JsonNode originalJsonNode = objectMapper.readTree(originalJson);
		UpdateEntityRequest request = new UpdateEntityRequest(headers, entityId, entityBody, expandedPayload, attrId);
		// pubilsh merged message
		// check if anything is changed.
		if (request.getStatus()) {
			if (directDB) {
				pushToDB(request);
			}
			new Thread() {
				public void run() {
					kafkaTemplate.send(ENTITY_UPDATE_TOPIC, entityId, DataSerializer.toJson(request));
					updateContext(request);
				}
			}.start();

		}
		logger.trace("partialUpdateEntity() :: completed");
		return request.getUpdateResult();
	}

	public boolean deleteAttribute(ArrayListMultimap<String, String> headers, String entityId, String attrId,
			String datasetId, String deleteAll) throws ResponseException, Exception {
		logger.trace("deleteAttribute() :: started");
		// get message channel for ENTITY_APPEND topic

		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed");
		}
		String tenantid = HttpUtils.getInternalTenant(headers);
		// get entity details
		Map<String, Object> entityBody = validateIdAndGetBody(entityId, tenantid);
		// get entity details from in-memory hashmap

		DeleteAttributeRequest request = new DeleteAttributeRequest(headers, entityId, entityBody, attrId, datasetId,
				deleteAll);
		if (directDB) {
			pushToDB(request);
		}
		new Thread() {
			public void run() {
				kafkaTemplate.send(ENTITY_DELETE_TOPIC, entityId, DataSerializer.toJson(request));
				updateContext(request);
			};
		}.start();

		logger.trace("deleteAttribute() :: completed");
		return true;
	}

	public boolean registerContext(EntityRequest request) throws URISyntaxException, IOException, ResponseException {
		// TODO needs rework as well
		logger.trace("registerContext() :: started");
		String entityBody = this.entityInfoDAO.getEntity(request.getId(),
				HttpUtils.getTenantFromHeaders(request.getHeaders()));
		CSourceRegistration contextRegistryPayload = this.getCSourceRegistrationFromJson(entityBody);
		CSourceRequest Csrequest = new CreateCSourceRequest(contextRegistryPayload, request.getHeaders());
		kafkaTemplate.send(CSOURCE_TOPIC, request.getId(), DataSerializer.toJson(Csrequest));
		logger.trace("registerContext() :: completed");
		return true;
	}

	private void updateContext(EntityRequest request) {
		// TODO needs rework as well
		logger.trace("updateContext() :: started");
		kafkaTemplate.send(CSOURCE_TOPIC, request.getId(), request.getWithSysAttrs());
		logger.trace("updateContext() :: completed");
	}

	private CSourceRegistration getCSourceRegistrationFromJson(String payload) throws URISyntaxException, IOException {
		logger.trace("getCSourceRegistrationFromJson() :: started");
		CSourceRegistration csourceRegistration = new CSourceRegistration();
		// csourceJsonBody = objectMapper.createObjectNode();
		// csourceJsonBody = objectMapper.readTree(entityBody);
		List<Information> information = new ArrayList<Information>();
		Information info = new Information();
		List<EntityInfo> entities = info.getEntities();
		Entity entity = DataSerializer.getEntity(payload);

		// Entity to CSourceRegistration conversion.
		csourceRegistration.setId(entity.getId());
		csourceRegistration.setEndpoint(MicroServiceUtils.getGatewayURL());
		// location node
		GeoProperty geoLocationProperty = entity.getLocation();
		if (geoLocationProperty != null) {
			csourceRegistration.setLocation(getCoveringGeoValue(geoLocationProperty));
		}

		// Information node
		Set<String> propertiesList = entity.getProperties().stream().map(Property::getIdString)
				.collect(Collectors.toSet());

		Set<String> relationshipsList = entity.getRelationships().stream().map(Relationship::getIdString)
				.collect(Collectors.toSet());

		entities.add(new EntityInfo(entity.getId(), null, entity.getType()));

		info.setPropertyNames(propertiesList);
		info.setRelationshipNames(relationshipsList);
		information.add(info);
		csourceRegistration.setInformation(information);

		// location node.

		TimeInterval timestamp = new TimeInterval();
		timestamp.setStartAt(new Date().getTime());
		csourceRegistration.setTimestamp(timestamp);
		logger.trace("getCSourceRegistrationFromJson() :: completed");
		return csourceRegistration;
	}

	private Geometry<?> getCoveringGeoValue(GeoProperty geoLocationProperty) {
		// TODO should be done better to cover the actual area
		return geoLocationProperty.getEntries().values().iterator().next().getGeoValue();
	}

	public URI getResourceURL(String resource) throws URISyntaxException {
		logger.trace("getResourceURL() :: started");
		URI uri = MicroServiceUtils.getGatewayURL();
		logger.trace("getResourceURL() :: completed");
		return new URI(uri.toString() + "/" + resource);
	}

}
