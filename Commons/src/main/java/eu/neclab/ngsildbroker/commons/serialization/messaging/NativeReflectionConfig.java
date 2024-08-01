package eu.neclab.ngsildbroker.commons.serialization.messaging;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdOptions;
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
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.DeleteSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.UpdateSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.OmitTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.PickTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.Query;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TemporalQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection(targets = { ArrayListMultimap.class, SubscriptionRequest.class, Subscription.class,
		NotificationParam.class, EndPoint.class, EntityInfo.class, LDTemporalQuery.class, LDGeoQuery.class,
		SyncMessage.class, AliveAnnouncement.class, GeoRelation.class, Context.class, GeoQueryTerm.class,
		TemporalQueryTerm.class, LanguageQueryTerm.class, AttrsQueryTerm.class, LanguageQueryTerm.class,
		QQueryTerm.class, ScopeQueryTerm.class, TypeQueryTerm.class, InternalNotification.class, BatchRequest.class,
		BaseRequest.class, DeleteSubscriptionRequest.class, UpdateSubscriptionRequest.class,
		JsonLdOptions.class, Query.class, PickTerm.class, OmitTerm.class, CSFQueryTerm.class }, serialization = true)
public class NativeReflectionConfig {

}
