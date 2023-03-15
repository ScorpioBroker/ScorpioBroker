package eu.neclab.ngsildbroker.commons.tools;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

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

	@SuppressWarnings("unchecked")
	public static LocalDateTime getLocalDateTime(Object dateTimeEntry) {
		return LocalDateTime.parse(((List<Map<String, String>>) dateTimeEntry).get(0).get(NGSIConstants.JSON_LD_VALUE),
				SerializationTools.informatter);
	}

}