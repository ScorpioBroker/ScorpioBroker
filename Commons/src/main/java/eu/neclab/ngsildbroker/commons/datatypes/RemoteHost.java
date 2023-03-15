package eu.neclab.ngsildbroker.commons.datatypes;

import io.vertx.mutiny.core.MultiMap;

public record RemoteHost(String host, String tenant, MultiMap headers, String cSourceId, boolean canDoSingleOp, boolean canDoBatchOp, int regMode) {

}
