package eu.neclab.ngsildbroker.commons.interfaces;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import io.smallrye.mutiny.Uni;

public interface BaseRequestHandler {
	public Uni<Void> handleBaseRequest(BaseRequest request);
}
