package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class BaseRequest {

	private String tenant;
	protected Map<String, Object> payload;
	private int requestType;
	private long sendTimestamp;
	private BatchInfo batchInfo;
	private String id;

	public BaseRequest() {

	}

	BaseRequest(String tenant, String id, Map<String, Object> requestPayload, BatchInfo batchInfo, int requestType) {
		super();
		this.tenant = tenant;
		this.requestType = requestType;
		this.batchInfo = batchInfo;
		this.id = id;
	}

	public int getRequestType() {
		return requestType;
	}

	public long getSendTimestamp() {
		return sendTimestamp;
	}

	public void setSendTimestamp(long sendTimestamp) {
		this.sendTimestamp = sendTimestamp;
	}

	public void setRequestType(int requestType) {
		this.requestType = requestType;
	}

	public BatchInfo getBatchInfo() {
		return batchInfo;
	}

	public void setBatchInfo(BatchInfo batchInfo) {
		this.batchInfo = batchInfo;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected static void setTemporalProperties(Object jsonNode, String createdAt, String modifiedAt,
			boolean rootOnly) {
		if (!(jsonNode instanceof Map)) {
			return;
		}
		Map<String, Object> objectNode = (Map<String, Object>) jsonNode;
		if (!createdAt.isEmpty()) {
			objectNode.remove(NGSIConstants.NGSI_LD_CREATED_AT);
			ArrayList<Object> tmp = getDateTime(createdAt);
			objectNode.put(NGSIConstants.NGSI_LD_CREATED_AT, tmp);
		}
		if (!modifiedAt.isEmpty()) {
			objectNode.remove(NGSIConstants.NGSI_LD_MODIFIED_AT);
			ArrayList<Object> tmp = getDateTime(modifiedAt);
			objectNode.put(NGSIConstants.NGSI_LD_MODIFIED_AT, tmp);
		}
		if (rootOnly) {
			return;
		}
		for (Entry<String, Object> entry : objectNode.entrySet()) {
			if (entry.getValue() instanceof List && !((List) entry.getValue()).isEmpty()) {
				List list = (List) entry.getValue();
				for (Object entry2 : list) {
					if (entry2 instanceof Map) {
						Map<String, Object> map = (Map<String, Object>) entry2;
						if (map.containsKey(NGSIConstants.JSON_LD_TYPE)
								&& map.get(NGSIConstants.JSON_LD_TYPE) instanceof List
								&& !((List) map.get(NGSIConstants.JSON_LD_TYPE)).isEmpty()
								&& ((List) map.get(NGSIConstants.JSON_LD_TYPE)).get(0).toString()
										.matches(NGSIConstants.REGEX_NGSI_LD_ATTR_TYPES)) {
							setTemporalProperties(map, createdAt, modifiedAt, rootOnly);
						}
					}
				}

			}
		}
	}

	private static ArrayList<Object> getDateTime(String createdAt) {
		ArrayList<Object> tmp = new ArrayList<Object>();
		HashMap<String, Object> tmp2 = new HashMap<String, Object>();
		tmp2.put(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME);
		tmp2.put(NGSIConstants.JSON_LD_VALUE, createdAt);
		tmp.add(tmp2);
		return tmp;
	}

	public Map<String, Object> getPayload() {
		return payload;
	}

	public String getTenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
	}

	public void setPayload(Map<String, Object> payload) {
		this.payload = payload;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public static Map<String, Object> addSysAttrs(Map<String, Object> resolved) {
		String now = SerializationTools.formatter.format(Instant.now());
		setTemporalProperties(resolved, now, now, false);
		return resolved;
	}

}
