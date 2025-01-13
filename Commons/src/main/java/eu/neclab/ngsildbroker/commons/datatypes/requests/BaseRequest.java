package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.collect.Lists;
import com.fasterxml.jackson.annotation.JsonProperty;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;

@JsonInclude(Include.NON_NULL)
public class BaseRequest implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8749175274463827625L;
	@JsonProperty(AppConstants.TENANT_SERIALIZATION_CHAR)
	protected String tenant;
	@JsonIgnore
	protected Map<String, List<Map<String, Object>>> payload;

	@JsonIgnore
	protected Map<String, List<Map<String, Object>>> prevPayload;

	@JsonIgnore
	private Map<String, Object> firstPayload;

	@JsonIgnore
	private String firstId;

	@JsonProperty(AppConstants.REQUESTTYPE_SERIALIZATION_CHAR)
	protected int requestType;

	@JsonProperty(AppConstants.SENDTIMESTAMP_SERIALIZATION_CHAR)
	protected long sendTimestamp = System.currentTimeMillis();

	@JsonIgnore
	protected Set<String> ids;

	@JsonProperty(AppConstants.ATTRIBNAME_SERIALIZATION_CHAR)
	protected String attribName;

	@JsonProperty(AppConstants.DATASETID_SERIALIZATION_CHAR)
	protected String datasetId;

	@JsonProperty(AppConstants.DELETEALL_SERIALIZATION_CHAR)
	protected boolean deleteAll;

	@JsonProperty(AppConstants.DISTRIBUTED_SERIALIZATION_CHAR)
	protected boolean distributed;

	@JsonProperty(AppConstants.NOOVERWRITE_SERIALIZATION_CHAR)
	protected boolean noOverwrite;

	@JsonProperty(AppConstants.INSTANCEID_SERIALIZATION_CHAR)
	protected String instanceId;

	@JsonProperty(AppConstants.ZIPPED_SERIALIZATION_CHAR)
	protected boolean zipped;

	public BaseRequest() {

	}

	protected BaseRequest(String tenant, String id, Map<String, Object> requestPayload, int requestType,
			boolean zipped) {
		super();
		this.tenant = tenant;
		this.requestType = requestType;
		this.ids = new HashSet<>(1);
		this.ids.add(id);
		this.zipped = zipped;
		if (requestPayload != null) {
			this.payload = new HashMap<>(1);
			List<Map<String, Object>> tmp = new ArrayList<>(1);
			tmp.add(requestPayload);
			this.payload.put(id, tmp);
		}

	}

	protected BaseRequest(String tenant, Set<String> ids, Map<String, List<Map<String, Object>>> requestPayload,
			int requestType, boolean zipped) {
		super();
		this.tenant = tenant;
		this.requestType = requestType;
		this.ids = ids;
		this.payload = requestPayload;
		this.zipped = zipped;

	}

	public String getTenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
	}

	public Map<String, List<Map<String, Object>>> getPayload() {
		return payload;
	}

	public void setPayload(Map<String, List<Map<String, Object>>> payload) {
		this.payload = payload;
	}

	public Map<String, List<Map<String, Object>>> getPrevPayload() {
		return prevPayload;
	}

	public void setPrevPayload(Map<String, List<Map<String, Object>>> prevPayload) {
		this.prevPayload = prevPayload;
	}

	public Map<String, Object> getFirstPayload() {
		if (this.firstPayload == null) {
			this.firstPayload = this.payload.get(getFirstId()).get(0);
		}
		return firstPayload;
	}

	public String getFirstId() {
		if (this.firstId == null) {
			this.firstId = this.ids.iterator().next();
		}
		return this.firstId;
	}

	public void setPrevPayloadFromSingle(String id, Map<String, Object> prevPayload) {
		if (prevPayload != null) {
			this.prevPayload = new HashMap<>(1);
			List<Map<String, Object>> tmp = new ArrayList<>(1);
			tmp.add(prevPayload);
			this.prevPayload.put(id, tmp);
		}
	}

	public void setPayloadFromSingle(String id, Map<String, Object> payload) {
		if (payload != null) {
			this.payload = new HashMap<>(1);
			List<Map<String, Object>> tmp = new ArrayList<>(1);
			tmp.add(payload);
			this.payload.put(id, tmp);
		}
	}

	public int getRequestType() {
		return requestType;
	}

	public void setRequestType(int requestType) {
		this.requestType = requestType;
	}

	public long getSendTimestamp() {
		return sendTimestamp;
	}

	public void setSendTimestamp(long sendTimestamp) {
		this.sendTimestamp = sendTimestamp;
	}

	public Set<String> getIds() {
		return ids;
	}

	public void setIds(Set<String> ids) {
		this.ids = ids;
	}

	public String getAttribName() {
		return attribName;
	}

	public void setAttribName(String attribName) {
		this.attribName = attribName;
	}

	public String getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(String datasetId) {
		this.datasetId = datasetId;
	}

	public boolean isDeleteAll() {
		return deleteAll;
	}

	public void setDeleteAll(boolean deleteAll) {
		this.deleteAll = deleteAll;
	}

	public boolean isDistributed() {
		return distributed;
	}

	public void setDistributed(boolean distributed) {
		this.distributed = distributed;
	}

	public boolean isNoOverwrite() {
		return noOverwrite;
	}

	public void setNoOverwrite(boolean noOverwrite) {
		this.noOverwrite = noOverwrite;
	}

	public String getInstanceId() {
		return instanceId;
	}

	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}

	public boolean isZipped() {
		return zipped;
	}

	public void setZipped(boolean zipped) {
		this.zipped = zipped;
	}

	@Override
	public String toString() {
		return "BaseRequest [tenant=" + tenant + ", payload=" + payload + ", prevPayload=" + prevPayload
				+ ", requestType=" + requestType + ", sendTimestamp=" + sendTimestamp + ", ids=" + ids + ", attribName="
				+ attribName + ", datasetId=" + datasetId + ", deleteAll=" + deleteAll + ", distributed=" + distributed
				+ ", noOverwrite=" + noOverwrite + "]";
	}

	public BaseRequest duplicate() {
		BaseRequest duplicate = new BaseRequest();
		duplicate.tenant = this.tenant;
		duplicate.payload = MicroServiceUtils.deepCopyIdMap(payload);
		duplicate.prevPayload = MicroServiceUtils.deepCopyIdMap(this.prevPayload);
		duplicate.requestType = this.requestType;
		duplicate.sendTimestamp = this.sendTimestamp;
		duplicate.ids = new HashSet<>(this.ids);
		duplicate.attribName = this.attribName;
		duplicate.datasetId = this.datasetId;
		duplicate.deleteAll = this.deleteAll;
		duplicate.distributed = this.distributed;
		duplicate.noOverwrite = this.noOverwrite;
		duplicate.instanceId = this.instanceId;
		duplicate.zipped = this.zipped;
		return duplicate;
	}

	public void addToPayload(String id, Map<String, Object> payloadToAdd) {
		List<Map<String, Object>> tmp = payload.get(id);
		if (tmp == null) {
			tmp = Lists.newArrayList();
			payload.put(id, tmp);
		}
		tmp.add(payloadToAdd);
	}

	public void addToPrevPayload(String id, Map<String, Object> payloadToAdd) {
		List<Map<String, Object>> tmp = prevPayload.get(id);
		if (tmp == null) {
			tmp = Lists.newArrayList();
			prevPayload.put(id, tmp);
		}
		tmp.add(payloadToAdd);
	}

	public BaseRequest copy() {
		return new BaseRequest(this);
	}

	private BaseRequest(BaseRequest original) {
		this.tenant = original.tenant;

		this.payload = deepCopyPayload(original.payload);
		this.prevPayload = deepCopyPayload(original.prevPayload);

		this.firstId = original.firstId;
		this.requestType = original.requestType;
		this.sendTimestamp = original.sendTimestamp;
		this.ids = new HashSet<>(original.ids);
		this.attribName = original.attribName;
		this.datasetId = original.datasetId;
		this.deleteAll = original.deleteAll;
		this.distributed = original.distributed;
		this.noOverwrite = original.noOverwrite;
		this.instanceId = original.instanceId;
		this.zipped = original.zipped;
	}

	private Map<String, List<Map<String, Object>>> deepCopyPayload(Map<String, List<Map<String, Object>>> payload) {
		if (payload == null) {
			return null;
		}
		Map<String, List<Map<String, Object>>> result = new HashMap<>(payload.size());
		for (Entry<String, List<Map<String, Object>>> entry : payload.entrySet()) {
			List<Map<String, Object>> tmp = new ArrayList<>(entry.getValue().size());
			for (int i = 0; i < entry.getValue().size(); i++) {
				tmp.add(MicroServiceUtils.deepCopyMap(entry.getValue().get(i)));
			}
			result.put(entry.getKey(), tmp);
		}
		return result;
	}

}
