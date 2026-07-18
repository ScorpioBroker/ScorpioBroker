package eu.neclab.ngsildbroker.commons.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.ParsedQueryParams;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.NgsiLdOperation;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class QueryParamParserTest {

	private static void assertErrorType(ResponseException e, ErrorType errorType) {
		assertEquals(errorType.getType(), e.getJson().get(NGSIConstants.ERROR_TYPE));
		assertEquals(errorType.getCode(), e.getErrorCode());
	}

	// ?count / ?count= / ?count=true / ?count=false

	@Test
	public void valuelessBooleanIsTrue() throws ResponseException {
		assertTrue(QueryParamParser.parse("type=T&count", NgsiLdOperation.QUERY_ENTITIES).getBoolean("count"));
		assertTrue(QueryParamParser.parse("type=T&count=", NgsiLdOperation.QUERY_ENTITIES).getBoolean("count"));
		assertTrue(QueryParamParser.parse("type=T&count=true", NgsiLdOperation.QUERY_ENTITIES).getBoolean("count"));
		assertFalse(QueryParamParser.parse("type=T&count=false", NgsiLdOperation.QUERY_ENTITIES).getBoolean("count"));
		assertFalse(QueryParamParser.parse("type=T", NgsiLdOperation.QUERY_ENTITIES).getBoolean("count"));
	}

	@Test
	public void valuelessNonBooleanIsEmptyString() throws ResponseException {
		ParsedQueryParams params = QueryParamParser.parse("attrs", NgsiLdOperation.QUERY_ENTITIES);
		assertTrue(params.has("attrs"));
		assertEquals("", params.getString("attrs"));
	}

	@Test
	public void invalidBooleanValueRejected() throws ResponseException {
		ParsedQueryParams params = QueryParamParser.parse("type=T&count=maybe", NgsiLdOperation.QUERY_ENTITIES);
		ResponseException e = assertThrows(ResponseException.class, () -> params.getBoolean("count"));
		assertErrorType(e, ErrorType.BadRequestData);
	}

	// '+' decodes to space like the previous stack (python-requests sends
	// spaces as '+'). Exception: in scopeQ '+' is the single-level wildcard,
	// so getScopeQ() keeps a raw '+' literal; %2B and %20 stay distinct.

	@Test
	public void plusDecodesToSpaceAndEncodedPlusStaysLiteral() throws ResponseException {
		ParsedQueryParams raw = QueryParamParser.parse("q=a+b", NgsiLdOperation.QUERY_ENTITIES);
		ParsedQueryParams encoded = QueryParamParser.parse("q=a%2Bb", NgsiLdOperation.QUERY_ENTITIES);
		assertEquals("a b", raw.getString("q"));
		assertEquals("a+b", encoded.getString("q"));
	}

	@Test
	public void scopeQKeepsRawPlusAsWildcard() throws ResponseException {
		assertEquals("/A/+/B",
				QueryParamParser.parse("scopeQ=/A/+/B", NgsiLdOperation.QUERY_ENTITIES).getScopeQ());
		assertEquals("/A/+/B",
				QueryParamParser.parse("scopeQ=/A/%2B/B", NgsiLdOperation.QUERY_ENTITIES).getScopeQ());
		assertEquals("/A/ /B",
				QueryParamParser.parse("scopeQ=/A/%20/B", NgsiLdOperation.QUERY_ENTITIES).getScopeQ());
		assertEquals("/A/#",
				QueryParamParser.parse("scopeQ=/A/#", NgsiLdOperation.QUERY_ENTITIES).getScopeQ());
		assertNull(QueryParamParser.parse("type=T", NgsiLdOperation.QUERY_ENTITIES).getScopeQ());
	}

	@Test
	public void plusInCoordinatesAndQDecodesToSpace() throws ResponseException {
		// python-requests style encoding of coordinates=[13.39, 52.55]
		assertEquals("[13.39, 52.55]", QueryParamParser
				.parse("coordinates=%5B13.39,+52.55%5D", NgsiLdOperation.QUERY_ENTITIES).getString("coordinates"));
		assertEquals("name==\"Main square\"", QueryParamParser
				.parse("q=name%3D%3D%22Main+square%22", NgsiLdOperation.QUERY_ENTITIES).getString("q"));
	}

	// q quote-stripping, raw and encoded identical

	@Test
	public void qQuoteStrippingRawAndEncoded() throws ResponseException {
		ParsedQueryParams raw = QueryParamParser.parse("q=brandName==\"Mercedes\"", NgsiLdOperation.QUERY_ENTITIES);
		ParsedQueryParams encoded = QueryParamParser.parse("q=brandName==%22Mercedes%22",
				NgsiLdOperation.QUERY_ENTITIES);
		assertEquals("brandName==Mercedes", raw.getQ());
		assertEquals("brandName==Mercedes", encoded.getQ());
	}

	// georel=near;maxDistance==2000 stays ONE value; separate param rejoins

	@Test
	public void georelWithSemicolonStaysOneValue() throws ResponseException {
		ParsedQueryParams raw = QueryParamParser.parse("georel=near;maxDistance==2000",
				NgsiLdOperation.QUERY_ENTITIES);
		assertEquals("near;maxDistance==2000", raw.getString("georel"));
		assertEquals("near;maxDistance==2000", raw.getGeorel());
		ParsedQueryParams encoded = QueryParamParser.parse("georel=near%3BmaxDistance%3D%3D2000",
				NgsiLdOperation.QUERY_ENTITIES);
		assertEquals("near;maxDistance==2000", encoded.getGeorel());
	}

	@Test
	public void georelRejoinsSeparateMaxDistance() throws ResponseException {
		// legacy separate-parameter form: value keeps the surplus '=' like the
		// old matrix-split workaround produced
		ParsedQueryParams doubleEq = QueryParamParser.parse("georel=near&maxDistance==2000",
				NgsiLdOperation.QUERY_ENTITIES);
		assertEquals("near;maxDistance==2000", doubleEq.getGeorel());
		ParsedQueryParams singleEq = QueryParamParser.parse("georel=near&maxDistance=2000",
				NgsiLdOperation.QUERY_ENTITIES);
		assertEquals("near;maxDistance==2000", singleEq.getGeorel());
		ParsedQueryParams minDist = QueryParamParser.parse("georel=near&minDistance=2000",
				NgsiLdOperation.QUERY_ENTITIES);
		assertEquals("near;minDistance==2000", minDist.getGeorel());
	}

	// NGSI-LD operator characters pass through character-identical, raw and
	// encoded. Identical input strings into the untouched QueryParser grammar
	// imply identical QQueryTerm/TypeQueryTerm trees.

	@Test
	public void operatorCharactersPassThrough() throws ResponseException {
		String q = "speed>50;brandName==\"Merc|edes\"";
		ParsedQueryParams raw = QueryParamParser.parse("q=" + q, NgsiLdOperation.QUERY_ENTITIES);
		assertEquals(q, raw.getString("q"));
		assertEquals("speed>50;brandName==Merc|edes", raw.getQ());
		ParsedQueryParams encoded = QueryParamParser.parse("q=speed%3E50%3BbrandName%3D%3D%22Merc%7Cedes%22",
				NgsiLdOperation.QUERY_ENTITIES);
		assertEquals(q, encoded.getString("q"));

		String type = "(A;B)|C";
		assertEquals(type,
				QueryParamParser.parse("type=(A;B)|C", NgsiLdOperation.QUERY_ENTITIES).getString("type"));
		assertEquals(type,
				QueryParamParser.parse("type=%28A%3BB%29%7CC", NgsiLdOperation.QUERY_ENTITIES).getString("type"));
	}

	@Test
	public void bracketsAndComparisonCharsRawAndEncodedIdentical() throws ResponseException {
		assertEquals("[8.68,49.41]", QueryParamParser.parse("coordinates=[8.68,49.41]",
				NgsiLdOperation.QUERY_ENTITIES).getString("coordinates"));
		assertEquals("[8.68,49.41]", QueryParamParser.parse("coordinates=%5B8.68,49.41%5D",
				NgsiLdOperation.QUERY_ENTITIES).getString("coordinates"));
		assertEquals("a<5", QueryParamParser.parse("q=a<5", NgsiLdOperation.QUERY_ENTITIES).getString("q"));
		assertEquals("a<5", QueryParamParser.parse("q=a%3C5", NgsiLdOperation.QUERY_ENTITIES).getString("q"));
	}

	// literal '#' is ordinary data (Smart Data Models URIs)

	@Test
	public void literalHashNotTruncated() throws ResponseException {
		ParsedQueryParams params = QueryParamParser.parse(
				"type=https://smartdatamodels.org/dataModel.Weather#WeatherObserved&limit=5",
				NgsiLdOperation.QUERY_ENTITIES);
		assertEquals("https://smartdatamodels.org/dataModel.Weather#WeatherObserved", params.getString("type"));
		assertEquals(Integer.valueOf(5), params.getInteger("limit"));
	}

	// allowlist enforcement

	@Test
	public void unknownParamIsInvalidRequest() {
		ResponseException e = assertThrows(ResponseException.class,
				() -> QueryParamParser.parse("invalidParams=x", NgsiLdOperation.QUERY_ENTITIES));
		assertErrorType(e, ErrorType.InvalidRequest);
	}

	@Test
	public void emptyAllowlistRejectsEverything() {
		ResponseException e = assertThrows(ResponseException.class,
				() -> QueryParamParser.parse("options=sysAttrs", NgsiLdOperation.CREATE_ENTITY));
		assertErrorType(e, ErrorType.InvalidRequest);
	}

	@Test
	public void duplicateParamIsBadRequestData() {
		ResponseException e = assertThrows(ResponseException.class,
				() -> QueryParamParser.parse("type=A&type=B", NgsiLdOperation.QUERY_ENTITIES));
		assertErrorType(e, ErrorType.BadRequestData);
	}

	// malformed percent-encoding

	@Test
	public void malformedPercentEncodingIsBadRequestData() {
		ResponseException hexError = assertThrows(ResponseException.class,
				() -> QueryParamParser.parse("q=a%G1", NgsiLdOperation.QUERY_ENTITIES));
		assertErrorType(hexError, ErrorType.BadRequestData);
		ResponseException truncated = assertThrows(ResponseException.class,
				() -> QueryParamParser.parse("q=a%2", NgsiLdOperation.QUERY_ENTITIES));
		assertErrorType(truncated, ErrorType.BadRequestData);
	}

	@Test
	public void multiBytePercentEncodingDecodes() throws ResponseException {
		assertEquals("München",
				QueryParamParser.parse("q=M%C3%BCnchen", NgsiLdOperation.QUERY_ENTITIES).getString("q"));
	}

	// integer accessors

	@Test
	public void nonNumericIntIsBadRequestData() throws ResponseException {
		ParsedQueryParams params = QueryParamParser.parse("limit=abc", NgsiLdOperation.QUERY_ENTITIES);
		ResponseException e = assertThrows(ResponseException.class, () -> params.getInteger("limit"));
		assertErrorType(e, ErrorType.BadRequestData);
	}

	@Test
	public void integerDefaults() throws ResponseException {
		ParsedQueryParams params = QueryParamParser.parse("type=T", NgsiLdOperation.QUERY_ENTITIES);
		assertNull(params.getInteger("limit"));
		assertEquals(0, params.getInt("offset", 0));
		assertEquals(-1, params.getInt("joinLevel", -1));
		// empty value behaves like absent, matching JAX-RS Integer binding
		assertNull(QueryParamParser.parse("limit=&type=T", NgsiLdOperation.QUERY_ENTITIES).getInteger("limit"));
	}

	// localOnly alias

	@Test
	public void localOnlyAliasForLocal() throws ResponseException {
		assertTrue(QueryParamParser.parse("localOnly=true", NgsiLdOperation.QUERY_ENTITIES).getLocal());
		assertTrue(QueryParamParser.parse("local=true", NgsiLdOperation.QUERY_ENTITIES).getLocal());
		assertFalse(QueryParamParser.parse("type=T", NgsiLdOperation.QUERY_ENTITIES).getLocal());
		// "local" wins over the alias when both are present
		assertFalse(QueryParamParser.parse("local=false&localOnly=true", NgsiLdOperation.QUERY_ENTITIES).getLocal());
	}

	// string defaults

	@Test
	public void stringDefaults() throws ResponseException {
		ParsedQueryParams params = QueryParamParser.parse("type=T", NgsiLdOperation.QUERY_ENTITIES);
		assertNull(params.getString("attrs"));
		assertEquals("", params.getString("containedBy", ""));
		assertEquals("true", params.getString("splitEntities", "true"));
		assertNull(params.getQ());
		assertNull(params.getGeorel());
	}

	// null/empty query strings are fine

	@Test
	public void nullAndEmptyQueryOk() throws ResponseException {
		assertFalse(QueryParamParser.parse((String) null, NgsiLdOperation.QUERY_ENTITIES).has("type"));
		assertFalse(QueryParamParser.parse("", NgsiLdOperation.QUERY_ENTITIES).has("type"));
		assertFalse(QueryParamParser.parse("&&", NgsiLdOperation.QUERY_ENTITIES).has("type"));
	}

	// raw values kept for forwarding

	@Test
	public void rawParamsKeepOriginalEncoding() throws ResponseException {
		ParsedQueryParams params = QueryParamParser.parse("q=a%3D%3D1&type=T", NgsiLdOperation.QUERY_ENTITIES);
		assertEquals("a%3D%3D1", params.getRawParams().get("q"));
		assertEquals("q=a%3D%3D1&type=T", params.toRawQueryString());
		assertEquals("a==1", params.getString("q"));
	}

	@Test
	public void rfc3986EncodeUsesPercent20() {
		assertEquals("a%20b%3D%3D1", QueryParamParser.rfc3986Encode("a b==1"));
	}

	// Purge Entities (6.4.3.3) allowlist

	@Test
	public void purgeAllowlistAcceptsSpecParams() throws ResponseException {
		ParsedQueryParams params = QueryParamParser.parse(
				"id=urn:a&type=T&idPattern=.*&attrs=speed&q=a==1&csf=b==2&geometry=Point"
						+ "&georel=near%3BmaxDistance%3D%3D2000&coordinates=[1,2]&geoproperty=location"
						+ "&scopeQ=/A&drop=speed&local=true",
				NgsiLdOperation.PURGE_ENTITIES);
		assertEquals("T", params.getString("type"));
		assertEquals("speed", params.getString("drop"));
		assertTrue(params.getLocal());
		assertEquals("name", QueryParamParser.parse("keep=name", NgsiLdOperation.PURGE_ENTITIES).getString("keep"));
	}

	@Test
	public void purgeRejectsUnknownAndNonPurgeParams() {
		ResponseException unknown = assertThrows(ResponseException.class,
				() -> QueryParamParser.parse("bogus=x", NgsiLdOperation.PURGE_ENTITIES));
		assertErrorType(unknown, ErrorType.InvalidRequest);
		// paging/projection params of QUERY_ENTITIES are not purge params
		ResponseException paging = assertThrows(ResponseException.class,
				() -> QueryParamParser.parse("type=T&limit=5", NgsiLdOperation.PURGE_ENTITIES));
		assertErrorType(paging, ErrorType.InvalidRequest);
		ResponseException projection = assertThrows(ResponseException.class,
				() -> QueryParamParser.parse("type=T&pick=name", NgsiLdOperation.PURGE_ENTITIES));
		assertErrorType(projection, ErrorType.InvalidRequest);
	}
}
