package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.results.Attrib;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import io.vertx.core.json.JsonObject;

public class AppendHistoryEntityRequest extends HistoryEntityRequest {

	private NGSILDOperationResult updateResult;

	public AppendHistoryEntityRequest(String tenant, Map<String, Object> resolved, String entityId, BatchInfo batchInfo)
			throws ResponseException {
		super(tenant, resolved, entityId, batchInfo, AppConstants.APPEND_REQUEST);
		updateResult = new NGSILDOperationResult(AppConstants.APPEND_REQUEST, entityId);
		createAppend();
	}

	public AppendHistoryEntityRequest(BaseRequest entityRequest) throws ResponseException, IOException {
		this(entityRequest.getTenant(), entityRequest.getPayload(), entityRequest.getId(),
				entityRequest.getBatchInfo());

	}

	@SuppressWarnings("unchecked")
	private void createAppend() {
		Set<Attrib> successAttribs = Sets.newHashSet();

		for (Entry<String, Object> entry : getPayload().entrySet()) {
			String datasetId = null;
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

					if (jsonElement.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
						datasetId = (String) jsonElement.get(NGSIConstants.NGSI_LD_DATA_SET_ID);
					}
					successAttribs.add(new Attrib(attribId, datasetId));
					instanceCount++;
				}
			}
			this.createdAt = now;
		}
		updateResult.addSuccess(new CRUDSuccess(null, null, null, successAttribs));
	}

	public NGSILDOperationResult getUpdateResult() {
		return updateResult;
	}

}
