package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class EntityMap {

	private Map<String, Integer> id2ListPosition = Maps.newHashMap();

	private List<EntityMapEntry> entityList = Lists.newArrayList();

	private boolean isDist;

	public EntityMap(boolean isDist) {
		this.isDist = isDist;
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

	public boolean isDist() {
		return isDist;
	}

}
