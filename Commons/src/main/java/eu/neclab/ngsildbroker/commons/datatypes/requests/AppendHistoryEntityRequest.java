package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.ArrayListMultimap;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import io.vertx.core.json.JsonObject;

public class AppendHistoryEntityRequest extends HistoryEntityRequest {

	private UpdateResult updateResult = new UpdateResult();

	public AppendHistoryEntityRequest(String tenant, Map<String, Object> resolved,
			String entityId) throws ResponseException {
		super(tenant, resolved, entityId, AppConstants.APPEND_REQUEST);
		setFinalPayload(resolved);
		createAppend();
	}

	public AppendHistoryEntityRequest(BaseRequest entityRequest) throws ResponseException, IOException {
		this(entityRequest.getTenant(), entityRequest.getRequestPayload(), entityRequest.getId());

	}

	@SuppressWarnings("unchecked")
	private void createAppend() {
		for (Entry<String, Object> entry : getRequestPayload().entrySet()) {
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
					jsonElement = setCommonTemporalProperties(jsonElement, now);
					storeEntry(getId(), null, null, now, attribId, JsonObject.mapFrom(jsonElement),
							EntityTools.getInstanceId(jsonElement), false);

					updateResult.addToUpdated(getId());
					instanceCount++;
				}
			}
			this.createdAt = now;
		}
	}

	public UpdateResult getUpdateResult() {
		return updateResult;
	}

}
