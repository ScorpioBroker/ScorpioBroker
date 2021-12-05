package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class AppendHistoryEntityRequest extends HistoryEntityRequest {

	public AppendHistoryEntityRequest(ArrayListMultimap<String, String> headers, Map<String, Object> resolved,
			String entityId) throws ResponseException {
		super(headers, resolved);
		this.id = entityId;
		createAppend();
	}

	public AppendHistoryEntityRequest(EntityRequest entityRequest) throws ResponseException, IOException {
		this(entityRequest.getHeaders(), (Map<String, Object>) JsonUtils.fromString(entityRequest.getWithSysAttrs()),
				entityRequest.getId());

	}

	protected void createAppend() {
		logger.trace("replace attribute in temporal entity");

		for (Entry<String, Object> entry : payload.entrySet()) {
			logger.debug("Key = " + entry.getKey() + " Value = " + entry.getValue());
			if (entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_CREATED_AT)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
				continue;
			}

			String attribId = entry.getKey();
			if (entry.getValue() instanceof List) {
				List<Map<String, Object>> valueArray = (List<Map<String, Object>>) entry.getValue();
				Integer instanceCount = 0;
				for (Map<String, Object> jsonElement : valueArray) {
					jsonElement = setCommonTemporalProperties(jsonElement, now, false);
					//
					Boolean overwriteOp = (instanceCount == 0); // if it's the first one, send the overwrite op to
																// delete current values
					try {
						storeEntry(id, null, null, now, attribId, JsonUtils.toPrettyString(jsonElement), overwriteOp);
					} catch (IOException e) {
						logger.error(e);
						//should never happen
					}

					instanceCount++;
				}
			}
			this.createdAt = now;
		}
		logger.trace("attribute replaced in temporalentity " + this.id);

	}

}
