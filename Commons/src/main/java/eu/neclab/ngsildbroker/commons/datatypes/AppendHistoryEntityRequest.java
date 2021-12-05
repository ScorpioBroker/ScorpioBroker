package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class AppendHistoryEntityRequest extends HistoryEntityRequest {
	
	public AppendHistoryEntityRequest(ArrayListMultimap<String, String> headers, String payload, String entityId) throws ResponseException {
		super(headers,payload);
		this.id = entityId;
		createAppend();
	}
	
	public AppendHistoryEntityRequest(EntityRequest entityRequest) throws ResponseException {
		this(entityRequest.getHeaders(), entityRequest.getWithSysAttrs(), entityRequest.getId());

	}

	protected void createAppend() {
		logger.trace("replace attribute in temporal entity");
		this.jsonObject = parser.parse(payload).getAsJsonObject();
		String opr="A";
		
		
		for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
			logger.debug("Key = " + entry.getKey() + " Value = " + entry.getValue());
			if (entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_CREATED_AT)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
				continue;
			}

			String attribId = entry.getKey();
			if (entry.getValue().isJsonArray()) {
				JsonArray valueArray = entry.getValue().getAsJsonArray();
				Integer instanceCount = 0;
				for (JsonElement jsonElement : valueArray) {
					jsonElement = setCommonTemporalProperties(jsonElement, now, false, opr);
					//
					Boolean overwriteOp = (instanceCount == 0); // if it's the first one, send the overwrite op to
																// delete current values
					storeEntry(id, null, null, now, attribId, jsonElement.toString(), overwriteOp);

					instanceCount++;
				}
			}
			this.createdAt = now;
		}
		logger.trace("attribute replaced in temporalentity " + this.id);

	}

}
