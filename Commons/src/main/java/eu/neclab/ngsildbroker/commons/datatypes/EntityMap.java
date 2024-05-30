package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class EntityMap {

	private Map<String, Integer> id2ListPosition = Maps.newHashMap();

	private List<EntityMapEntry> entityList = Lists.newArrayList();

	private boolean onlyFullEntitiesDistributed;

	private boolean regEmpty;

	private boolean noRootLevelRegEntry;
	
	private boolean changed = false;

	private String id;

	private Map<String, Object> linkedMaps = Maps.newHashMap();

	private Map<String, Set<String>> remoteHost2Ids = Maps.newHashMap();

	public EntityMap(String id, boolean onlyFullEntitiesDistributed, boolean regEmpty, boolean noRootLevelRegEntry) {
		this.id = id;
		this.onlyFullEntitiesDistributed = onlyFullEntitiesDistributed;
		this.regEmpty = regEmpty;
		this.noRootLevelRegEntry = noRootLevelRegEntry;
	}

	public EntityMapEntry getEntry(String entityId) {
		Integer listPos = id2ListPosition.get(entityId);
		EntityMapEntry result;
		if (listPos == null) {
			listPos = entityList.size();
			result = new EntityMapEntry(entityId);
			entityList.add(result);
			id2ListPosition.put(entityId, listPos);
		} else {
			result = entityList.get(listPos);
		}
		return result;
	}

	public List<EntityMapEntry> getSubMap(int from, int to) {
		List<EntityMapEntry> entityListRes = Lists.newArrayList();
		if (to > entityList.size()) {
			to = entityList.size();
		}

		if (from >= 0 && to <= entityList.size() && from <= to) {
			entityListRes = entityList.subList(from, to);
		}
		return entityListRes;
	}

	public Map<String, Integer> getId2ListPosition() {
		return id2ListPosition;
	}

	public void setId2ListPosition(Map<String, Integer> id2ListPosition) {
		this.id2ListPosition = id2ListPosition;
	}

	public List<EntityMapEntry> getEntityList() {
		return entityList;
	}

	public void setEntityList(List<EntityMapEntry> entityList) {
		this.entityList = entityList;
	}

	public boolean isEmpty() {
		return entityList.isEmpty();
	}

	public long size() {
		return entityList.size();
	}

	public boolean onlyFullEntitiesDistributed() {
		return onlyFullEntitiesDistributed;
	}

	public boolean isRegEmpty() {
		return regEmpty;
	}

	public String getId() {
		return id;
	}

	public boolean isNoRootLevelRegEntry() {
		return noRootLevelRegEntry;
	}

	public boolean isOnlyFullEntitiesDistributed() {
		return onlyFullEntitiesDistributed;
	}

	public void setOnlyFullEntitiesDistributed(boolean onlyFullEntitiesDistributed) {
		this.onlyFullEntitiesDistributed = onlyFullEntitiesDistributed;
	}

	public void setRegEmpty(boolean regEmpty) {
		this.regEmpty = regEmpty;
	}

	public void setNoRootLevelRegEntry(boolean noRootLevelRegEntry) {
		this.noRootLevelRegEntry = noRootLevelRegEntry;
	}

	public Map<String, Object> getLinkedMaps() {
		return linkedMaps;
	}

	public void addLinkedMap(String cSourceId, String entityMapId) {
		this.linkedMaps.put(cSourceId, entityMapId);
	}

	public void setLinkedMaps(Map<String, Object> linkedMaps) {
		this.linkedMaps = linkedMaps;
	}

	public void removeEntries(Set<String> entityIds) {
		List<Integer> toDelete = new ArrayList<>(entityIds.size());
		entityIds.forEach(entityId -> {
			Integer listPos = id2ListPosition.get(entityId);
			toDelete.add(listPos);
		});
		Collections.sort(toDelete);
		for (int i = 0; i < toDelete.size(); i++) {
			int index = toDelete.get(i) - i;
			EntityMapEntry removed = entityList.remove(index);
			for (QueryRemoteHost remoteHost : removed.remoteHosts) {
				Set<String> ids = remoteHost2Ids.get(remoteHost.host);
				ids.remove(removed.entityId);
				if (ids.isEmpty()) {
					remoteHost2Ids.remove(remoteHost.host);
					linkedMaps.remove(remoteHost.cSourceId);
				}
			}
		}
		id2ListPosition = Maps.newHashMap();
		for (int i = 0; i < entityList.size(); i++) {
			EntityMapEntry entityMapEntry = entityList.get(i);
			id2ListPosition.put(entityMapEntry.getEntityId(), i);
		}
	}

	public boolean isChanged() {
		return changed;
	}

	public void setChanged(boolean changed) {
		this.changed = changed;
	}
	
	

}
