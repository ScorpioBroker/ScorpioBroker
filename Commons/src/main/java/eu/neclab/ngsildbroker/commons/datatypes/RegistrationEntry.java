package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.locationtech.spatial4j.SpatialPredicate;
import org.locationtech.spatial4j.shape.Shape;

import com.google.common.collect.Sets;

import io.smallrye.mutiny.tuples.Tuple2;

public record RegistrationEntry(String cId, String eId, String eIdp, String type, String eProp, String eRel,
		Shape location, String[] scopes, long expiresAt, int regMode, boolean createEntity, boolean updateEntity,
		boolean appendAttrs, boolean updateAttrs, boolean deleteAttrs, boolean deleteEntity, boolean createBatch,
		boolean upsertBatch, boolean updateBatch, boolean deleteBatch, boolean upsertTemporal,
		boolean appendAttrsTemporal, boolean deleteAttrsTemporal, boolean updateAttrsTemporal,
		boolean deleteAttrInstanceTemporal, boolean deleteTemporal, boolean mergeEntity, boolean replaceEntity,
		boolean replaceAttrs, boolean mergeBatch, boolean retrieveEntity, boolean queryEntity, boolean queryBatch,
		boolean retrieveTemporal, boolean queryTemporal, boolean retrieveEntityTypes, boolean retrieveEntityTypeDetails,
		boolean retrieveEntityTypeInfo, boolean retrieveAttrTypes, boolean retrieveAttrTypeDetails,
		boolean retrieveAttrTypeInfo, boolean createSubscription, boolean updateSubscription,
		boolean retrieveSubscription, boolean querySubscription, boolean deleteSubscription, RemoteHost host) {

	/**
	 * 
	 * @param id
	 * @param type
	 * @param prop
	 * @param rel
	 * @param originalScopes
	 * @return Matching types and matching scopes if no scopes are in the registry-
	 *         the original scopes will be returned scopes can be null if there no
	 *         scopes in the entity if no match is possible it will return null
	 */
	public Tuple2<Set<String>, Set<String>> matches(String id, List<String> types, String prop, String rel,
			Object originalScopes, Shape location) {
		if (id != null && this.eId != null && !id.equals(eId)) {
			return null;
		}
		if (this.eIdp != null && !id.matches(eIdp)) {
			return null;
		}
		if (prop != null && eProp != null && !prop.equals(eProp)) {
			return null;
		}
		if (rel != null && eRel != null && !rel.equals(eRel)) {
			return null;
		}
		if ((location != null && this.location == null) || (location != null && this.location != null
				&& SpatialPredicate.IsWithin.evaluate(location, this.location))) {
			return null;
		}
		if (this.type != null && types != null && !types.contains(this.type)) {
			return null;
		}
		if (this.scopes != null && originalScopes == null) {
			return null;
		}
		Set<String> resultScopes = getOverlap((List<Map<String, String>>) originalScopes);
		Set<String> resultType = null;
		if (this.type == null && types != null) {
			resultType = Sets.newHashSet(types);
		} else if (types != null) {
			resultType = Sets.newHashSet(this.type);
		}

		return Tuple2.of(resultType, resultScopes);
	}

	private Set<String> getOverlap(List<Map<String, String>> originalScopes) {
		// TODO Auto-generated method stub
		return null;
	}

	public static List<RegistrationEntry> fromRegPayload(Map<String, Object> payload) {
		// TODO Auto-generated method stub
		return null;
	}

}
