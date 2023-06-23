package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

import java.util.Map;

public class MergePatchRequest extends EntityRequest {


    public MergePatchRequest(String tenant, String id, Map<String, Object> payload,
                             BatchInfo batchInfo) {
        super(tenant, id, payload, batchInfo, AppConstants.MERGE_PATCH_REQUEST);
    }

}
