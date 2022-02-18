package eu.neclab.ngsildbroker.commons.interfaces;

import eu.neclab.ngsildbroker.commons.datatypes.GeoqueryRel;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public interface StorageFunctionsInterface {

	String translateNgsildQueryToSql(QueryParams qp);

	String translateNgsildQueryToCountResult(QueryParams qp);

	String typesAndAttributeQuery(QueryParams qp);

	String translateNgsildGeoqueryToPostgisQuery(GeoqueryRel georel, String geometry, String coordinates,
			String geoproperty);

}
