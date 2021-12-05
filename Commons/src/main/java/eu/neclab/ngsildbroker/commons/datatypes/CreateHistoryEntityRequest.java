package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonParseException;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class CreateHistoryEntityRequest extends HistoryEntityRequest {

	private boolean fromEntity;

	private URI uriId;
	private int attributeCount;

	public boolean isFromEntity() {
		return fromEntity;
	}

	public void setFromEntity(boolean fromEntity) {
		this.fromEntity = fromEntity;
	}

	public URI getUriId() {
		return uriId;
	}

	public void setUriId(URI uriId) {
		this.uriId = uriId;
	}

	public int getAttributeCount() {
		return attributeCount;
	}

	public void setAttributeCount(int attributeCount) {
		this.attributeCount = attributeCount;
	}

	/**
	 * Serialization constructor
	 * 
	 * @param entityRequest
	 * @throws ResponseException
	 * @throws IOException
	 * @throws JsonParseException
	 */
	public CreateHistoryEntityRequest(EntityRequest entityRequest) throws ResponseException, IOException {
		this(entityRequest.getHeaders(), (Map<String, Object>) JsonUtils.fromString(entityRequest.getWithSysAttrs()),
				true);
	}

	public CreateHistoryEntityRequest(ArrayListMultimap<String, String> headers, Map<String, Object> resolved,
			boolean fromEntity) throws ResponseException {
		super(headers, resolved);
		this.fromEntity = fromEntity;
		try {
			createTemporalEntity(resolved, fromEntity);
		} catch (ResponseException e) {
			throw e;
		} catch (Exception e) {
			throw new ResponseException(e.getMessage());
		}
		// super(AppConstants.OPERATION_CREATE_HISTORY_ENTITY, headers);
	}

	private void createTemporalEntity(Map<String, Object> resolved, boolean fromEntity)
			throws ResponseException, Exception {
		this.attributeCount = 0;
		if (resolved.get(NGSIConstants.NGSI_LD_CREATED_AT) == null
				|| resolved.get(NGSIConstants.NGSI_LD_CREATED_AT) == null) {
			ArrayList<Object> temp = new ArrayList<Object>();
			HashMap<String, Object> tempObj = new HashMap<String, Object>();
			tempObj.put(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME);
			tempObj.put(NGSIConstants.JSON_LD_VALUE, now);
			temp.add(tempObj);
			if (resolved.get(NGSIConstants.NGSI_LD_CREATED_AT) == null) {
				resolved.put(NGSIConstants.NGSI_LD_CREATED_AT, temp);
			}
			if (resolved.get(NGSIConstants.NGSI_LD_MODIFIED_AT) == null) {
				resolved.put(NGSIConstants.NGSI_LD_MODIFIED_AT, temp);
			}
		}

		this.id = (String) resolved.get(NGSIConstants.JSON_LD_ID);
		this.type = (String) ((List) resolved.get(NGSIConstants.JSON_LD_TYPE)).get(0);
		this.createdAt = (String) ((List<Map<String, Object>>) resolved.get(NGSIConstants.NGSI_LD_CREATED_AT)).get(0)
				.get(NGSIConstants.JSON_LD_VALUE);
		this.modifiedAt = (String) ((List<Map<String, Object>>) resolved.get(NGSIConstants.NGSI_LD_MODIFIED_AT)).get(0)
				.get(NGSIConstants.JSON_LD_VALUE);

		Integer attributeCount = 0;

		for (Entry<String, Object> entry : resolved.entrySet()) {
			logger.debug("Key = " + entry.getKey() + " Value = " + entry.getValue());
			if (entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_CREATED_AT)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_MODIFIED_AT)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_INSTANCE_ID)) {
				continue;
			}

			String attribId = entry.getKey();
			// Boolean createTemporalEntityIfNotExists = (attributeCount == 0); // if it's
			// the first attribute, create the
			// //
			// temporalentity record

			if (entry.getValue() instanceof List) {
				List<Map<String, Object>> valueArray = (List<Map<String, Object>>) entry.getValue();
				// TODO check if changes in the array are reflect in the object
				for (Map<String, Object> jsonElement : valueArray) {
					jsonElement = setCommonTemporalProperties(jsonElement, now, fromEntity);
					storeEntry(id, type, createdAt, modifiedAt, attribId, jsonElement.toString(), false);
					// pushAttributeToKafka(id, type, createdAt, modifiedAt, attribId,
					// jsonElement.toString(), createTemporalEntityIfNotExists, false);
				}
			}
			attributeCount++;
		}
		this.payload = resolved;
		// attributeCount++; //move out }if(attributeCount==0)

		// { // create empty temporalentity (no attributes) TemporalEntityStorageKey
		// tesk = new TemporalEntityStorageKey(id); tesk.setEntityType(type);
		// tesk.setEntityCreatedAt(createdAt); tesk.setEntityModifiedAt(modifiedAt);
		// String messageKey = DataSerializer.toJson(tesk); logger.debug(" message key "
		// + messageKey + " payload element: empty"); /*
		// kafkaOperations.pushToKafka(producerChannels.temporalEntityWriteChannel(),
		// messageKey.getBytes(), "".getBytes());

		// }logger.trace("temporal entity created "+id);
		this.uriId = new URI(AppConstants.HISTORY_URL + id);
	}

}
