package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;

public class UpdateHistoryEntityRequest extends HistoryEntityRequest {

	private String oldEntry;
	private String instanceId;
	private String resolvedAttrId;

	public UpdateHistoryEntityRequest(ArrayListMultimap<String, String> headers, Map<String, Object> resolved,
			String entityId, String resolvedAttrId, String instanceId, String oldEntry) throws ResponseException {
		super(headers, resolved, entityId, AppConstants.UPDATE_REQUEST);
		this.oldEntry = oldEntry;
		this.resolvedAttrId = resolvedAttrId;
		this.instanceId = instanceId;
		createUpdate();
	}

	@SuppressWarnings("unchecked")
	public UpdateHistoryEntityRequest(BaseRequest entityRequest) throws IOException {
		setHeaders(entityRequest.getHeaders());
		Map<String, Object> jsonObject = entityRequest.getRequestPayload();// (Map<String, Object>)
																			// JsonUtils.fromString(entityRequest.getWithSysAttrs());
		for (Entry<String, Object> entry : jsonObject.entrySet()) {
			if (entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_CREATED_AT)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
				continue;
			}
			String attribIdPayload = entry.getKey();

			if (entry.getValue() instanceof List) {
				List<Map<String, Object>> valueArray = (List<Map<String, Object>>) entry.getValue();
				for (Map<String, Object> jsonElement : valueArray) {
					jsonElement = setCommonTemporalProperties(jsonElement, now);
					storeEntry(entityRequest.getId(), null, null, now, attribIdPayload,
							JsonUtils.toPrettyString(jsonElement), EntityTools.getInstanceId(jsonElement), false);
				}
			} else if(entry.getValue() instanceof Map) {
                Map<String, Object> jsonElement = (Map<String, Object>) entry.getValue();
                jsonElement = setCommonTemporalProperties(jsonElement, now);
                storeEntry(entityRequest.getId(), null, null, now, attribIdPayload,
                        JsonUtils.toPrettyString(jsonElement), EntityTools.getInstanceId(jsonElement), false);
            }
		}

	}

	@SuppressWarnings("unchecked")
	private void createUpdate() throws ResponseException {
		this.createdAt = now;
		String instanceIdAdd = null;
		List<Map<String, Object>> jsonArray = null;
		try {
			jsonArray = (List<Map<String, Object>>) JsonUtils.fromString(oldEntry);
			this.createdAt = (String) ((List<Map<String, Object>>) ((List<Map<String, Object>>) ((List<Map<String, Object>>) jsonArray)
					.get(0).get(resolvedAttrId)).get(0).get(NGSIConstants.NGSI_LD_CREATED_AT)).get(0)
							.get(NGSIConstants.JSON_LD_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}
		for (Entry<String, Object> entry : getRequestPayload().entrySet()) {
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

			if (entry.getValue() instanceof List) {
				List<Map<String, Object>> valueArray = (List<Map<String, Object>>) entry.getValue();
				for (Map<String, Object> jsonElement : valueArray) {
					if (jsonElement.get(NGSIConstants.NGSI_LD_INSTANCE_ID) != null) {
						if (!((List<Map<String, Object>>) jsonElement.get(NGSIConstants.NGSI_LD_INSTANCE_ID)).get(0)
								.get(NGSIConstants.JSON_LD_ID).equals(instanceId)) {
							throw new ResponseException(ErrorType.InvalidRequest,
									"instanceId in payload and in URL must be the same");
						}
					} else {
						instanceIdAdd = (String) ((List<Map<String, Object>>) ((List<Map<String, Object>>) jsonArray
								.get(0).get(resolvedAttrId)).get(0).get(NGSIConstants.NGSI_LD_INSTANCE_ID)).get(0)
										.get(NGSIConstants.JSON_LD_ID);
						jsonElement = setTemporalPropertyinstanceId(jsonElement, NGSIConstants.NGSI_LD_INSTANCE_ID,
								instanceIdAdd);
					}
					jsonElement = setTemporalProperty(jsonElement, NGSIConstants.NGSI_LD_CREATED_AT, createdAt);
					jsonElement = setTemporalProperty(jsonElement, NGSIConstants.NGSI_LD_MODIFIED_AT, now);
					try {
						storeEntry(getId(), null, null, now, attribIdPayload, JsonUtils.toPrettyString(jsonElement),
								EntityTools.getInstanceId(jsonElement), false);
					} catch (IOException e) {
						// Should never happen
					}
				}
			}
		}
	}

}
