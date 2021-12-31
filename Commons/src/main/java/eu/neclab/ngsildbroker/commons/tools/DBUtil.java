package eu.neclab.ngsildbroker.commons.tools;


import java.net.URI;

public class DBUtil {

	public static String databaseURLFromPostgresJdbcUrl(String url, String newDbName) {
		try {
			String cleanURI = url.substring(5);

			URI uri = URI.create(cleanURI);
			return "jdbc:" + uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort() + "/" + newDbName;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}