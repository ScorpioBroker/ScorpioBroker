package eu.neclab.ngsildbroker.commons.storage;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.checkerframework.checker.nullness.qual.Nullable;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.github.filosganga.geogson.model.Geometry;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.GeoRelation;
import eu.neclab.ngsildbroker.commons.datatypes.GeoqueryRel;
import eu.neclab.ngsildbroker.commons.datatypes.LDGeoQuery;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.QueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageInterface;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;

public abstract class InternalStorageReplacement implements StorageInterface {

	private HashMap<String, Map<String, Object>> entityId2Entity = Maps.newHashMap();
	private HashMap<String, Map<String, Object>> entityId2KVEntity = Maps.newHashMap();
	private HashMap<String, Map<String, Object>> entityId2SysAttrsEntity = Maps.newHashMap();
	private HashMultimap<String, String> tenant2EntityIds = HashMultimap.create();
	private HashMultimap<String, String> entityId2Scopes = HashMultimap.create();
	private HashMultimap<String, String> type2EntityIds = HashMultimap.create();
	private HashMultimap<String, String> attribName2EntityIds = HashMultimap.create();
	private HashMultimap<Geometry<?>, String> location2EntityIds = HashMultimap.create();
	private HashMultimap<Date, String> observedAt2EntityIds = HashMultimap.create();
	private HashMultimap<Date, String> createdAt2EntityIds = HashMultimap.create();
	private HashMultimap<Date, String> modifiedAt2EntityIds = HashMultimap.create();

	private HashMap<String, Map<String, Object>> regId2RegistryEntry = Maps.newHashMap();
	private HashMultimap<String, String> tenant2RegIds = HashMultimap.create();
	private HashMultimap<String, String> type2RegIds = HashMultimap.create();
	private HashMultimap<Geometry<?>, String> location2RegIds = HashMultimap.create();

	private HashMap<String, Map<String, Object>> tempEntityId2RegistryEntry = Maps.newHashMap();
	private HashMultimap<String, String> tenant2tempEntityIds = HashMultimap.create();
	private HashMultimap<String, String> type2tempEntityIds = HashMultimap.create();
	private HashMultimap<Geometry<?>, String> geometry2TempEntityIds = HashMultimap.create();
	private HashMultimap<Date, String> observedAt2TempEntityIds = HashMultimap.create();
	private HashMultimap<Date, String> createdAt2TempEntityIds = HashMultimap.create();
	private HashMultimap<Date, String> modifiedAt2TempEntityIds = HashMultimap.create();

	protected QueryResult query(QueryParams qp, int endPoint) throws ResponseException {
		switch (endPoint) {
		case AppConstants.QUERY_ENDPOINT:
			return queryEntities(qp);
		case AppConstants.REGISTRY_ENDPOINT:
			return queryRegEntries(qp);
		case AppConstants.HISTORY_ENDPOINT:

			break;

		default:
			break;
		}
		return null;
	}

	private QueryResult queryRegEntries(QueryParams qp) {
		// TODO Auto-generated method stub
		return null;
	}

	private QueryResult queryEntities(QueryParams qp) {
		HashSet<String> entityIds = new HashSet<>();
		List<Map<String, String>> entities = qp.getEntities();
		if (entities != null) {
			for (Map<String, String> entityInfo : entities) {
				String[] ids = null;
				String[] types = null;
				String idPattern = null;
				for (Entry<String, String> entry : entityInfo.entrySet()) {
					switch (entry.getKey()) {
					case NGSIConstants.JSON_LD_ID:
						ids = entry.getValue().split(",");
						break;
					case NGSIConstants.JSON_LD_TYPE:
						types = entry.getValue().split(",");
						break;
					case NGSIConstants.NGSI_LD_ID_PATTERN:
						idPattern = entry.getValue();
						break;

					default:
						break;
					}
				}
				HashSet<String> typeBasedIds = new HashSet<>();
				for (String type : types) {
					typeBasedIds.addAll(this.type2EntityIds.get(type));
				}
				if (ids != null) {
					HashSet<String> filterIds = new HashSet<>();
					for (String id : ids) {
						if (typeBasedIds.contains(id)) {
							filterIds.add(id);
						}
					}
					typeBasedIds = filterIds;
				} else if (idPattern != null) {
					HashSet<String> filterIds = new HashSet<>();
					for (String id : typeBasedIds) {
						if (id.matches(idPattern)) {
							filterIds.add(id);
						}
					}
					typeBasedIds = filterIds;
				}
				entityIds.addAll(typeBasedIds);
			}
		}
		if (qp.getAttrs() != null) {
			;
			String[] attrs = qp.getAttrs().split(",");
			boolean prefiltered = entityIds.isEmpty();
			if (prefiltered) {
				HashSet<String> temp = Sets.newHashSet();
				for (String attr : attrs) {
					Set<String> ids = attribName2EntityIds.get(attr);
					for (String id : ids) {
						if (entityIds.contains(id)) {
							temp.add(id);
						}
					}
				}
				entityIds = temp;
			} else {
				entityIds.addAll(Arrays.asList(attrs));
			}
		}
		String tenant = qp.getTenant();
		HashSet<String> tenantBasedIds = Sets.newHashSet();
		for (String entry : tenant2EntityIds.get(tenant)) {
			if (entityIds.contains(entry)) {
				tenantBasedIds.add(entry);
			}
		}
		entityIds = tenantBasedIds;
		if (qp.getScopeQ() != null) {
			String scopeQ = qp.getScopeQ();
			HashSet<String> temp = Sets.newHashSet();
			for (Entry<String, String> entry : entityId2Scopes.entries()) {
				if (!entityIds.contains(entry.getKey())) {
					continue;
				}
				if (entry.getValue().matches(scopeQ)) {
					temp.add(entry.getKey());
				}
			}
			entityIds = temp;
		}
		if (qp.getGeorel() != null) {
			GeoqueryRel gqr = qp.getGeorel();
			String coordinates = qp.getCoordinates();
		}

		if (qp.getLdQuery() != null) {
			QueryTerm query = qp.getLdQuery();
			HashSet<String> temp = Sets.newHashSet();
			for (String id : entityIds) {
				try {
					if (query.calculate(EntityTools.getBaseProperties(entityId2Entity.get(id)))) {
						temp.add(id);
					}
				} catch (ResponseException e) {
					// TODO logger
				}
			}
			entityIds = temp;
		}
		List<String> resultEntities = Lists.newArrayList();
		for (String id : entityIds) {
			try {
				resultEntities.add(JsonUtils.toString(entityId2Entity.get(id)));
			} catch (JsonGenerationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return new QueryResult(resultEntities, null, ErrorType.None, -1, true);
	}

	@Override
	public boolean storeTemporalEntity(HistoryEntityRequest request) throws SQLException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean storeRegistryEntry(CSourceRequest request) throws SQLException {
		synchronized (tenant2RegIds) {
			String id = request.getId();
			String tenant = request.getTenant();
			if (request instanceof DeleteCSourceRequest) {
				Map<String, Object> payload = regId2RegistryEntry.remove(id);
				tenant2RegIds.remove(tenant, id);
				Set<String> types = EntityTools.getRegisteredTypes(payload);
				for (String type : types) {
					type2RegIds.remove(type, id);
				}
				Geometry<?> location = getLocationGeometry(payload, true);
				if (location != null) {
					location2EntityIds.remove(location, id);
				}

			} else {
				Map<String, Object> payload = request.getFinalPayload();
				tenant2RegIds.put(request.getTenant(), id);
				regId2RegistryEntry.put(id, payload);
				Set<String> types = EntityTools.getRegisteredTypes(payload);
				for (String type : types) {
					type2RegIds.put(type, id);
				}
				Geometry<?> location = getLocationGeometry(payload, false);
				if (location != null) {
					location2RegIds.put(location, id);
				}
			}
		}
		return true;
	}

	@Override
	public boolean storeEntity(EntityRequest request) throws SQLTransientConnectionException {
		synchronized (tenant2EntityIds) {
			String id = request.getId();
			String tenant = request.getTenant();
			if (request instanceof DeleteEntityRequest) {
				Map<String, Object> payload = entityId2Entity.remove(id);
				tenant2EntityIds.remove(tenant, id);
				List<String> types = (List<String>) payload.get(NGSIConstants.JSON_LD_TYPE);
				for (String type : types) {
					type2EntityIds.remove(type, id);
				}
				Geometry<?> geometry = getLocationGeometry(payload, true);
				if (geometry != null) {
					location2EntityIds.remove(geometry, id);
				}
				Date observedAt = getDateTime(payload, NGSIConstants.NGSI_LD_OBSERVED_AT);
				if (observedAt != null) {
					observedAt2EntityIds.remove(observedAt, id);
				}
				createdAt2EntityIds.remove(getDateTime(payload, NGSIConstants.NGSI_LD_CREATED_AT), id);
				modifiedAt2EntityIds.remove(getDateTime(payload, NGSIConstants.NGSI_LD_MODIFIED_AT), id);
			} else {

				try {
					Map<String, Object> payload = (Map<String, Object>) JsonUtils
							.fromString(request.getEntityWithoutSysAttrs());
					Map<String, Object> payloadWithSysAttrs = (Map<String, Object>) JsonUtils
							.fromString(request.getWithSysAttrs());
					Map<String, Object> payloadKV = (Map<String, Object>) JsonUtils.fromString(request.getKeyValue());

					tenant2EntityIds.put(request.getTenant(), id);
					getAttribNames(id, payload);
					entityId2Entity.put(id, payload);
					entityId2SysAttrsEntity.put(id, payloadWithSysAttrs);
					entityId2SysAttrsEntity.put(id, payloadKV);

					List<String> types = (List<String>) payload.get(NGSIConstants.JSON_LD_TYPE);
					for (String type : types) {
						type2EntityIds.put(type, id);
					}
					Geometry<?> geometry = getLocationGeometry(payload, true);
					if (geometry != null) {
						location2EntityIds.put(geometry, id);
					}
					Date observedAt = getDateTime(payload, NGSIConstants.NGSI_LD_OBSERVED_AT);
					if (observedAt != null) {
						observedAt2EntityIds.put(observedAt, id);
					}
					createdAt2EntityIds.put(getDateTime(payload, NGSIConstants.NGSI_LD_CREATED_AT), id);
					modifiedAt2EntityIds.put(getDateTime(payload, NGSIConstants.NGSI_LD_MODIFIED_AT), id);
				} catch (JsonParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return true;
	}

	private void getAttribNames(String id, Map<String, Object> payload) {
		for (String key : payload.keySet()) {
			if (key.equals(NGSIConstants.JSON_LD_CONTEXT) || key.equals(NGSIConstants.JSON_LD_ID)
					|| key.equals(NGSIConstants.JSON_LD_TYPE)) {
				continue;
			}
			attribName2EntityIds.put(key, id);
		}
	}

	private Date getDateTime(Map<String, Object> payload, String ngsiLdObservedAt) {
		// TODO Auto-generated method stub
		return null;
	}

	private Geometry<?> getLocationGeometry(Map<String, Object> payload, boolean realGeoproperty) {
		// TODO Auto-generated method stub
		return null;
	}

}
