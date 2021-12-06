package com.github.jsonldjava.core;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public class Constants {
	;
	public final static ArrayListMultimap<Integer, String> allowedDateTimes = ArrayListMultimap.create();
	static {
		allowedDateTimes.put(AppConstants.ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_OBSERVED_AT);
		allowedDateTimes.put(AppConstants.ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_OBSERVED_AT);
		allowedDateTimes.put(AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_OBSERVED_AT);
		allowedDateTimes.put(AppConstants.ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_OBSERVED_AT);
		allowedDateTimes.put(AppConstants.ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_CREATED_AT);
		allowedDateTimes.put(AppConstants.ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_MODIFIED_AT);
	}

	public final static ArrayListMultimap<Integer, String> allowedScalars = ArrayListMultimap.create();
	static {
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_SUBSCRIPTION_NAME);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_DESCRIPTION);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_TIME_INTERVAL);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_QUERY);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_IS_ACTIVE);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_EXPIRES);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_THROTTLING);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_WATCHED_ATTRIBUTES);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_ID_PATTERN);
		allowedScalars.put(AppConstants.ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_HAS_OBJECT);
		allowedScalars.put(AppConstants.ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_HAS_VALUE);
		allowedScalars.put(AppConstants.ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_COORDINATES);
		allowedScalars.put(AppConstants.ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_UNIT_CODE);
		allowedScalars.put(AppConstants.ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_DATA_SET_ID);
		allowedScalars.put(AppConstants.ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_HAS_OBJECT);
		allowedScalars.put(AppConstants.ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_HAS_VALUE);
		allowedScalars.put(AppConstants.ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_COORDINATES);
		allowedScalars.put(AppConstants.ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_UNIT_CODE);
		allowedScalars.put(AppConstants.ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_DATA_SET_ID);
		allowedScalars.put(AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_HAS_OBJECT);
		allowedScalars.put(AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_HAS_VALUE);
		allowedScalars.put(AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_COORDINATES);
		allowedScalars.put(AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_UNIT_CODE);
		allowedScalars.put(AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_DATA_SET_ID);
		allowedScalars.put(AppConstants.ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_HAS_OBJECT);
		allowedScalars.put(AppConstants.ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_HAS_VALUE);
		allowedScalars.put(AppConstants.ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_COORDINATES);
		allowedScalars.put(AppConstants.ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_UNIT_CODE);
		allowedScalars.put(AppConstants.ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_DATA_SET_ID);
	}
	public final static ArrayListMultimap<Integer, String> allowedTopLevel = ArrayListMultimap.create();
	static {
		allowedTopLevel.putAll(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD,
				allowedScalars.get(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD));
	}

}
