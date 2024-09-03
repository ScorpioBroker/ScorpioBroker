package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.Serializable;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * @author hebgen
 *
 */
@JsonSerialize
@JsonDeserialize
public class SyncMessage implements Serializable {

	public static final int NORMAL_SUB = 0;
	public static final int REG_SUB = 1;

	/**
	 *
	 */
	private static final long serialVersionUID = 1572704886071996626L;
	private String syncId;
	private String subId;
	private String tenant;
	private int subType;
	private int requestType;

	public SyncMessage(String syncId, String subId, String tenant, int requestType, int subType) {
		super();
		this.syncId = syncId;
		this.subId = subId;
		this.tenant = tenant;
		this.subType = subType;
		this.requestType = requestType;
	}

	SyncMessage() {
		// TODO Auto-generated constructor stub
	}

	public int getSubType() {
		return subType;
	}

	public void setSubType(int subType) {
		this.subType = subType;
	}

	public String getSyncId() {
		return syncId;
	}

	public void setSyncId(String syncId) {
		this.syncId = syncId;
	}

	public String getSubId() {
		return subId;
	}

	public void setSubId(String subId) {
		this.subId = subId;
	}

	public String getTenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
	}

	public int getRequestType() {
		return requestType;
	}

	public void setRequestType(int requestType) {
		this.requestType = requestType;
	}

	@Override
	public String toString() {
		return "SyncMessage [syncId=" + syncId + ", subId=" + subId + ", tenant=" + tenant + ", subType=" + subType
				+ ", requestType=" + requestType + "]";
	}

}
