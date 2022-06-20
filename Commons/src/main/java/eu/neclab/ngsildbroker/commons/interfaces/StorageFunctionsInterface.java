package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.ArrayList;

import com.google.common.collect.Lists;

import eu.neclab.ngsildbroker.commons.datatypes.GeoqueryRel;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.SqlConnection;

public interface StorageFunctionsInterface {
	Uni<RowSet<Row>> translateNgsildQueryToSql(QueryParams qp, SqlConnection conn);

	Uni<RowSet<Row>> translateNgsildQueryToCountResult(QueryParams qp, SqlConnection conn);

	Uni<RowSet<Row>> typesAndAttributeQuery(QueryParams qp, SqlConnection conn);

	Tuple3<String, ArrayList<Object>, Integer> translateNgsildGeoqueryToPostgisQuery(GeoqueryRel georel,
			String geometry, String coordinates, String geoproperty, int currentCount) throws ResponseException;

	String getAllIdsQuery();

	String getEntryQuery();

	static Tuple3<String, ArrayList<Object>, Integer> getSQLList(String attrs, int currentCount) {
		StringBuilder sqlQuery = new StringBuilder();
		ArrayList<Object> replacements = Lists.newArrayList(attrs.split(","));
		int newCount = replacements.size() + currentCount;
		for (int i = currentCount; i < newCount; i++) {
			sqlQuery.append('$');
			sqlQuery.append(i);
			sqlQuery.append(',');
		}
		return Tuple3.of(sqlQuery.substring(0, sqlQuery.length() - 1), replacements, newCount);
	}

}
