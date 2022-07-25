package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class AppendCSourceRequest extends CSourceRequest {

	private UpdateResult updateResult = new UpdateResult();

	public AppendCSourceRequest(ArrayListMultimap<String, String> headers, String registrationId,
			Map<String, Object> originalRegistration, Map<String, Object> update, String[] options)
			throws ResponseException {
		super(headers, registrationId, update, AppConstants.OPERATION_APPEND_ENTITY);
		setFinalPayload(appendRequest(originalRegistration, update, options));
	}

	private Map<String, Object> appendRequest(Map<String, Object> originalRegistration, Map<String, Object> update,
			String[] options) throws ResponseException {
		boolean overwrite = true;
		if (options != null) {
			for (String option : options) {
				if (option.isBlank()) {
					continue;
				}
				if (option.equalsIgnoreCase(NGSIConstants.NO_OVERWRITE_OPTION)) {
					overwrite = false;
				} else {
					throw new ResponseException(ErrorType.BadRequestData, option + " is an invalid option");
				}
			}
		}
		for (Entry<String, Object> entry : update.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if (key.equalsIgnoreCase(NGSIConstants.JSON_LD_CONTEXT) || key.equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
					|| key.equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)) {
				continue;
			}
			if (overwrite) {
				originalRegistration.put(key, value);
				updateResult.addToUpdated(key);
			} else {
				if (!originalRegistration.containsKey(key)) {
					originalRegistration.put(key, value);
					updateResult.addToUpdated(key);
				} else {
					updateResult.addToNotUpdated(key, "already exists and was not overwriten");
				}
			}
		}
		return originalRegistration;
	}

	public UpdateResult getUpdateResult() {
		return updateResult;
	}

}
