package eu.neclab.ngsildbroker.commons.interfaces;

import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceBaseRequest;
import io.smallrye.mutiny.Uni;

public interface CSourceHandler {
	public Uni<Void> handleRegistryChange(CSourceBaseRequest req);

}
