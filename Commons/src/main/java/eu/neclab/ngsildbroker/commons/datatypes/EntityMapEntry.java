package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;

import com.google.common.collect.Lists;

public class EntityMapEntry {

	String entityId;
	List<QueryRemoteHost> remoteHosts = Lists.newArrayList();

	public EntityMapEntry(String entityId) {
		this.entityId = entityId;
	}

	public String getEntityId() {
		return entityId;
	}

	public void setEntityId(String entityId) {
		this.entityId = entityId;
	}

	public List<QueryRemoteHost> getRemoteHosts() {
		return remoteHosts;
	}

	public void setRemoteHosts(List<QueryRemoteHost> remoteHosts) {
		this.remoteHosts = remoteHosts;
	}

}
