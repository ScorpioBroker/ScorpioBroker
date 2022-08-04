package eu.neclab.ngsildbroker.commons.serialization.messaging;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection(targets = { ArrayListMultimap.class, Subscription.class }, serialization = true)
public class ArraylistMultimapReflectionConfig {

}
