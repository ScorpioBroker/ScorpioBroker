package eu.neclab.ngsildbroker.commons.storage;

/**
 * Features of the connected PostgreSQL server that change the generated SQL.
 * Initialized once at startup by {@link ConnectionManager}; static because the
 * query terms using it are not CDI beans.
 */
public final class PgCapabilities {

	public static final int PG17 = 170000;

	// default: assume PG17+ (behaviour before the detection was added)
	private static volatile boolean jsonPathStringMethod = true;

	private PgCapabilities() {
	}

	/**
	 * @return true if jsonpath supports the {@code .string()} method (PostgreSQL
	 *         17+)
	 */
	public static boolean hasJsonPathStringMethod() {
		return jsonPathStringMethod;
	}

	/**
	 * @param serverVersionNum value of {@code server_version_num}, e.g. 140013
	 * @param override         {@code auto}, {@code true} or {@code false}; anything
	 *                         else is treated as {@code auto}
	 * @return the resulting mode
	 */
	public static boolean init(int serverVersionNum, String override) {
		if ("true".equalsIgnoreCase(override)) {
			jsonPathStringMethod = true;
		} else if ("false".equalsIgnoreCase(override)) {
			jsonPathStringMethod = false;
		} else {
			jsonPathStringMethod = serverVersionNum >= PG17;
		}
		return jsonPathStringMethod;
	}

	public static void reset() {
		jsonPathStringMethod = true;
	}
}
