package eu.neclab.ngsildbroker.commons.serialization.messaging;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection(targets = { ArrayListMultimap.class, SubscriptionRequest.class, Subscription.class })
public class NativeReflectionConfig {

}
