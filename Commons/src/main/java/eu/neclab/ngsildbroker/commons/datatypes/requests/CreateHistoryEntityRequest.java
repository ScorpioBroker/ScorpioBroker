package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonParseException;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;

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
	public CreateHistoryEntityRequest(BaseRequest entityRequest) throws Exception {
		this(entityRequest.getHeaders(), entityRequest.getFinalPayload(), true);
	}

	public CreateHistoryEntityRequest(ArrayListMultimap<String, String> headers, Map<String, Object> resolved,
			boolean fromEntity) throws Exception {
		super(headers, resolved, (String) resolved.get(NGSIConstants.JSON_LD_ID), AppConstants.CREATE_REQUEST);
		this.fromEntity = fromEntity;
		createTemporalEntity(resolved, fromEntity);
	}

	@SuppressWarnings("unchecked")
	private void createTemporalEntity(Map<String, Object> resolved, boolean fromEntity) throws Exception {
		this.attributeCount = 0;
		resolved = setCommonDateProperties(resolved, now);
		this.type = ((List<String>) resolved.get(NGSIConstants.JSON_LD_TYPE)).get(0);
		this.createdAt = (String) ((List<Map<String, Object>>) resolved.get(NGSIConstants.NGSI_LD_CREATED_AT)).get(0)
				.get(NGSIConstants.JSON_LD_VALUE);
		this.modifiedAt = (String) ((List<Map<String, Object>>) resolved.get(NGSIConstants.NGSI_LD_MODIFIED_AT)).get(0)
				.get(NGSIConstants.JSON_LD_VALUE);

		Integer attributeCount = 0;

		for (Entry<String, Object> entry : resolved.entrySet()) {
			if (entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_CREATED_AT)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_MODIFIED_AT)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_INSTANCE_ID)) {
				continue;
			}

			String attribId = entry.getKey();
			if (entry.getValue() instanceof List) {
				List<Map<String, Object>> valueArray = (List<Map<String, Object>>) entry.getValue();
				// TODO check if changes in the array are reflect in the object
				for (Map<String, Object> jsonElement : valueArray) {
					jsonElement = setCommonTemporalProperties(jsonElement, now);
					storeEntry(getId(), type, createdAt, modifiedAt, attribId, JsonUtils.toPrettyString(jsonElement),
							EntityTools.getInstanceId(jsonElement), false);

				}
			}
			attributeCount++;
		}
		this.uriId = new URI(AppConstants.HISTORY_URL + getId());
	}

}
