package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class UpdateHistoryEntityRequest extends HistoryEntityRequest {

	private String oldEntry;
	private String instanceId;
	private String resolvedAttrId;

	public UpdateHistoryEntityRequest(ArrayListMultimap<String, String> headers, String payload, String entityId,
			String resolvedAttrId, String instanceId, String oldEntry) throws ResponseException {
		super(headers, payload);
		this.id = entityId;
		this.oldEntry = oldEntry;
		this.resolvedAttrId = resolvedAttrId;
		this.instanceId = instanceId;
		createUpdate();
	}

	public UpdateHistoryEntityRequest(EntityRequest entityRequest) {
		
		logger.trace("Listener handleEntityUpdate...");
		// logger.debug("Received key: " + key);
		// String payload = new String(message);
		setHeaders(entityRequest.getHeaders());
		final JsonObject jsonObject = parser.parse(entityRequest.getWithSysAttrs()).getAsJsonObject();
		for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
			logger.debug("Key = " + entry.getKey() + " Value = " + entry.getValue());
			if (entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_CREATED_AT)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
				continue;
			}
			String attribIdPayload = entry.getKey();

			if (entry.getValue().isJsonArray()) {
				JsonArray valueArray = entry.getValue().getAsJsonArray();
				for (JsonElement jsonElement : valueArray) {
					jsonElement = setCommonTemporalProperties(jsonElement, now, true);
					storeEntry(entityRequest.getId(), null, null, now, attribIdPayload, jsonElement.toString(), false);
				}
			}
		}

	}

	private void createUpdate() throws ResponseException {
		this.createdAt = now;
		String instanceIdAdd = null;
		JsonArray jsonArray = null;
		try {
			jsonArray = parser.parse(oldEntry).getAsJsonArray();
			this.createdAt = jsonArray.get(0).getAsJsonObject().get(resolvedAttrId).getAsJsonArray().get(0)
					.getAsJsonObject().get(NGSIConstants.NGSI_LD_CREATED_AT).getAsJsonArray().get(0).getAsJsonObject()
					.get(NGSIConstants.JSON_LD_VALUE).getAsString();
		} catch (Exception e) {
			e.printStackTrace();
			logger.warn("original createdAt element not found, using current timestamp");
		}

		logger.debug(
				"modify attribute instance in temporal entity " + this.id + " - " + resolvedAttrId + " - " + createdAt);

		this.jsonObject = parser.parse(payload).getAsJsonObject();

		for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
			logger.debug("Key = " + entry.getKey() + " Value = " + entry.getValue());
			if (entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_CREATED_AT)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
				continue;
			}
			String attribIdPayload = entry.getKey();
			if (!attribIdPayload.equals(resolvedAttrId)) {
				throw new ResponseException(ErrorType.InvalidRequest,
						"attribute id in payload and in URL must be the same: " + attribIdPayload + " (payload) / "
								+ resolvedAttrId + " (URL)");
			}

			if (entry.getValue().isJsonArray()) {
				JsonArray valueArray = entry.getValue().getAsJsonArray();
				for (JsonElement jsonElement : valueArray) {
					if (jsonElement.getAsJsonObject().get(NGSIConstants.NGSI_LD_INSTANCE_ID) != null) {
						if (!jsonElement.getAsJsonObject().get(NGSIConstants.NGSI_LD_INSTANCE_ID).getAsJsonArray()
								.get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_ID).getAsString()
								.equals(instanceId)) {
							throw new ResponseException(ErrorType.InvalidRequest,
									"instanceId in payload and in URL must be the same");
						}
					} else {
						instanceIdAdd = jsonArray.get(0).getAsJsonObject().get(resolvedAttrId).getAsJsonArray().get(0)
								.getAsJsonObject().get(NGSIConstants.NGSI_LD_INSTANCE_ID).getAsJsonArray().get(0)
								.getAsJsonObject().get(NGSIConstants.JSON_LD_ID).getAsString();
						jsonElement = setTemporalPropertyinstanceId(jsonElement, NGSIConstants.NGSI_LD_INSTANCE_ID,
								instanceIdAdd);
					}
					jsonElement = setTemporalProperty(jsonElement, NGSIConstants.NGSI_LD_CREATED_AT, createdAt);
					jsonElement = setTemporalProperty(jsonElement, NGSIConstants.NGSI_LD_MODIFIED_AT, now);
					storeEntry(id, null, null, now, attribIdPayload, jsonElement.toString(), false);
				}
			}
		}
		logger.trace("instance modified in temporalentity " + this.id);
	}

}
