package eu.neclab.ngsildbroker.commons.interfaces;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;

import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public interface StorageInterface {

	QueryResult query(QueryParams qp) throws ResponseException;

	boolean storeTemporalEntity(HistoryEntityRequest request) throws SQLException;

	boolean storeRegistryEntry(CSourceRequest request) throws SQLException;

	boolean storeEntity(EntityRequest request) throws SQLTransientConnectionException;

}
