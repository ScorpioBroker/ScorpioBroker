package eu.neclab.ngsildbroker.commons.serialization.messaging;

import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class GeoQueryTermDeserializer extends ObjectMapperDeserializer<GeoQueryTerm> {
	public GeoQueryTermDeserializer() {
		super(GeoQueryTerm.class);
	}

}
