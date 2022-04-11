package com.github.jsonldjava.core;

import com.google.common.collect.HashMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public class Constants {

	public final static HashMultimap<Integer, String> allowedDateTimes = HashMultimap.create();
	static {
		allowedDateTimes.put(AppConstants.ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_OBSERVED_AT);
		allowedDateTimes.put(AppConstants.ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_OBSERVED_AT);
		allowedDateTimes.put(AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_OBSERVED_AT);
		allowedDateTimes.put(AppConstants.ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_OBSERVED_AT);
		allowedDateTimes.put(AppConstants.ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_CREATED_AT);
		allowedDateTimes.put(AppConstants.ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_MODIFIED_AT);

		allowedDateTimes.put(AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_OBSERVED_AT);
		allowedDateTimes.put(AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_OBSERVED_AT);
		allowedDateTimes.put(AppConstants.TEMP_ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_OBSERVED_AT);
		allowedDateTimes.put(AppConstants.TEMP_ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_CREATED_AT);
		allowedDateTimes.put(AppConstants.TEMP_ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_MODIFIED_AT);

		allowedDateTimes.put(AppConstants.CSOURCE_REG_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_TIMESTAMP_START);
		allowedDateTimes.put(AppConstants.CSOURCE_REG_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_TIMESTAMP_END);
		allowedDateTimes.put(AppConstants.CSOURCE_REG_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_EXPIRES);
		allowedDateTimes.put(AppConstants.CSOURCE_REG_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_TIMESTAMP_START);
	}
	public final static HashMultimap<Integer, String> allowedUrls = HashMultimap.create();
	static {
		allowedUrls.put(AppConstants.ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_DATA_SET_ID);
		allowedUrls.put(AppConstants.ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_DATA_SET_ID);
		allowedUrls.put(AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_DATA_SET_ID);
		allowedUrls.put(AppConstants.ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_DATA_SET_ID);

		allowedUrls.put(AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_DATA_SET_ID);
		allowedUrls.put(AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_DATA_SET_ID);
		allowedUrls.put(AppConstants.TEMP_ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_DATA_SET_ID);
		allowedUrls.put(AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_INSTANCE_ID);
		allowedUrls.put(AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_INSTANCE_ID);
		allowedUrls.put(AppConstants.TEMP_ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_INSTANCE_ID);
		allowedUrls.put(AppConstants.CSOURCE_REG_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_ENDPOINT);
		allowedUrls.put(AppConstants.CSOURCE_REG_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_ENDPOINT);
	}

	public final static HashMultimap<Integer, String> allowedScalars = HashMultimap.create();
	static {
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_SUBSCRIPTION_NAME);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_DESCRIPTION);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_TIME_INTERVAL);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_QUERY);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_CSF);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_IS_ACTIVE);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_EXPIRES);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_THROTTLING);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_WATCHED_ATTRIBUTES);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_ID_PATTERN);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_IS_ACTIVE);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_EXPIRES);
		allowedScalars.put(AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_SCOPE_Q);

		allowedScalars.put(AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_SUBSCRIPTION_NAME);
		allowedScalars.put(AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_DESCRIPTION);
		allowedScalars.put(AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_TIME_INTERVAL);
		allowedScalars.put(AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_QUERY);
		allowedScalars.put(AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_CSF);
		allowedScalars.put(AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_IS_ACTIVE);
		allowedScalars.put(AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_EXPIRES);
		allowedScalars.put(AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_THROTTLING);
		allowedScalars.put(AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_WATCHED_ATTRIBUTES);
		allowedScalars.put(AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_ID_PATTERN);
		allowedScalars.put(AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_IS_ACTIVE);
		allowedScalars.put(AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_EXPIRES);
		allowedScalars.put(AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_SCOPE_Q);

		allowedScalars.put(AppConstants.ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_HAS_OBJECT);
		allowedScalars.put(AppConstants.ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_HAS_VALUE);
		allowedScalars.put(AppConstants.ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_COORDINATES);
		allowedScalars.put(AppConstants.ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_UNIT_CODE);
		allowedScalars.put(AppConstants.ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_DATA_SET_ID);
		allowedScalars.put(AppConstants.ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_SCOPE);
		allowedScalars.put(AppConstants.ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_HAS_OBJECT);
		allowedScalars.put(AppConstants.ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_HAS_VALUE);
		allowedScalars.put(AppConstants.ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_COORDINATES);
		allowedScalars.put(AppConstants.ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_UNIT_CODE);
		allowedScalars.put(AppConstants.ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_DATA_SET_ID);
		allowedScalars.put(AppConstants.ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_SCOPE);
		allowedScalars.put(AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_HAS_OBJECT);
		allowedScalars.put(AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_HAS_VALUE);
		allowedScalars.put(AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_COORDINATES);
		allowedScalars.put(AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_UNIT_CODE);
		allowedScalars.put(AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_DATA_SET_ID);
		allowedScalars.put(AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_SCOPE);
		allowedScalars.put(AppConstants.ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_HAS_OBJECT);
		allowedScalars.put(AppConstants.ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_HAS_VALUE);
		allowedScalars.put(AppConstants.ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_COORDINATES);
		allowedScalars.put(AppConstants.ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_UNIT_CODE);
		allowedScalars.put(AppConstants.ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_DATA_SET_ID);
		allowedScalars.put(AppConstants.ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_SCOPE);
		allowedScalars.put(AppConstants.CSOURCE_REG_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_PROPERTIES);
		allowedScalars.put(AppConstants.CSOURCE_REG_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_RELATIONSHIPS);
		allowedScalars.put(AppConstants.CSOURCE_REG_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_ID_PATTERN);
		allowedScalars.put(AppConstants.CSOURCE_REG_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_COORDINATES);
		allowedScalars.put(AppConstants.CSOURCE_REG_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_TENANT);
		allowedScalars.put(AppConstants.CSOURCE_REG_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_TIME_STAMP);
		allowedScalars.put(AppConstants.CSOURCE_REG_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_SCOPE);
		allowedScalars.put(AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_HAS_OBJECT);
		allowedScalars.put(AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_HAS_VALUE);
		allowedScalars.put(AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_COORDINATES);
		allowedScalars.put(AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_UNIT_CODE);
		allowedScalars.put(AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_DATA_SET_ID);
		allowedScalars.put(AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, NGSIConstants.NGSI_LD_SCOPE);
		allowedScalars.put(AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_HAS_OBJECT);
		allowedScalars.put(AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_HAS_VALUE);
		allowedScalars.put(AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_COORDINATES);
		allowedScalars.put(AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_UNIT_CODE);
		allowedScalars.put(AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_DATA_SET_ID);
		allowedScalars.put(AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_SCOPE);
		allowedScalars.put(AppConstants.TEMP_ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_HAS_OBJECT);
		allowedScalars.put(AppConstants.TEMP_ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_HAS_VALUE);
		allowedScalars.put(AppConstants.TEMP_ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_COORDINATES);
		allowedScalars.put(AppConstants.TEMP_ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_UNIT_CODE);
		allowedScalars.put(AppConstants.TEMP_ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_DATA_SET_ID);
		allowedScalars.put(AppConstants.TEMP_ENTITY_RETRIEVED_PAYLOAD, NGSIConstants.NGSI_LD_SCOPE);
		allowedScalars.put(AppConstants.CSOURCE_REG_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_PROPERTIES);
		allowedScalars.put(AppConstants.CSOURCE_REG_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_RELATIONSHIPS);
		allowedScalars.put(AppConstants.CSOURCE_REG_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_ID_PATTERN);
		allowedScalars.put(AppConstants.CSOURCE_REG_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_COORDINATES);
		allowedScalars.put(AppConstants.CSOURCE_REG_UPDATE_PAYLOAD, NGSIConstants.NGSI_LD_SCOPE);

	}
}
