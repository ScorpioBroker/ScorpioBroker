package eu.neclab.ngsildbroker.commons.serialization.messaging;

import com.google.common.collect.ArrayListMultimap;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection(targets = { ArrayListMultimap.class }, serialization = true)
public class ArraylistMultimapReflectionConfig {

}
