package eu.neclab.ngsildbroker.commons.serialization.messaging;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.AliveAnnouncement;
import eu.neclab.ngsildbroker.commons.datatypes.EndPoint;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.GeoRelation;
import eu.neclab.ngsildbroker.commons.datatypes.LDGeoQuery;
import eu.neclab.ngsildbroker.commons.datatypes.LDTemporalQuery;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SyncMessage;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection(targets = { ArrayListMultimap.class, SubscriptionRequest.class, Subscription.class,
		NotificationParam.class, EndPoint.class, EntityInfo.class, LDTemporalQuery.class, LDGeoQuery.class,
		SyncMessage.class, AliveAnnouncement.class, GeoRelation.class }, serialization = true)
public class NativeReflectionConfig {

}
