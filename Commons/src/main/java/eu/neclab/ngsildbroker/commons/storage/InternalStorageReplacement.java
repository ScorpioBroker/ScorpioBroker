package eu.neclab.ngsildbroker.commons.storage;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.checkerframework.checker.nullness.qual.Nullable;

import com.github.filosganga.geogson.model.Geometry;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageInterface;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;

public abstract class InternalStorageReplacement implements StorageInterface {

	private HashMap<String, Map<String, Object>> entityId2Entity = Maps.newHashMap();
	private HashMultimap<String, String> tenant2EntityIds = HashMultimap.create();
	private HashMultimap<String, String> type2EntityIds = HashMultimap.create();
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
			break;
		case AppConstants.REGISTRY_ENDPOINT:

			break;
		case AppConstants.HISTORY_ENDPOINT:

			break;

		default:
			break;
		}
		return null;
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
				Map<String, Object> payload = request.getFinalPayload();
				tenant2EntityIds.put(request.getTenant(), id);
				entityId2Entity.put(id, payload);
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
			}
		}
		return true;
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
