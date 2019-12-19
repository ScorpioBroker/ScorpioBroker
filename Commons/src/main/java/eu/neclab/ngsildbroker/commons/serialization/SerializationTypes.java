package eu.neclab.ngsildbroker.commons.serialization;

import java.lang.reflect.Type;
import java.util.List;

import com.google.common.reflect.TypeToken;

import eu.neclab.ngsildbroker.commons.datatypes.Entity;

public class SerializationTypes {
	public static final Type entitiesType = new TypeToken<List<Entity>>() {
	}.getType();
	public static final Type entityType = new TypeToken<Entity>() {
	}.getType();
}
