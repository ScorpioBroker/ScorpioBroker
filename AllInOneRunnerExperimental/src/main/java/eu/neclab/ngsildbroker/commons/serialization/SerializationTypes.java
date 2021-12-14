package eu.neclab.ngsildbroker.commons.serialization;

import java.lang.reflect.Type;
import java.util.List;

import com.google.common.reflect.TypeToken;

import eu.neclab.ngsildbroker.commons.datatypes.Entity;

public class SerializationTypes {
	public static final Type entitiesType = new TypeToken<List<Entity>>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = -5506963234573679498L;
	}.getType();
	public static final Type entityType = new TypeToken<Entity>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = -5107931552416075705L;
	}.getType();
}
