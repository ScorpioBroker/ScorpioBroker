package eu.neclab.ngsildbroker.commons.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class HttpUtilsRawQueryParamTest {

	@Test
	public void decodesOnce() {
		// ^\d+$ encoded, %2B must end up as a literal +
		assertEquals("status~=\"^\\d+$\"",
				HttpUtils.getRawQueryParam("type=T&q=status~=%22%5E%5Cd%2B%24%22", "q"));
	}

	@Test
	public void keepsUnencodedSemicolonAndParentheses() {
		assertEquals("a==1;status~=\"^(?!active$).*\"",
				HttpUtils.getRawQueryParam("q=a==1;status~=\"^(?!active$).*\"&limit=10", "q"));
	}

	@Test
	public void encodedAmpersandStaysInValue() {
		assertEquals("name~=\"a&b\"", HttpUtils.getRawQueryParam("q=name~=%22a%26b%22&limit=1", "q"));
	}

	@Test
	public void matchesWholeParameterName() {
		assertEquals("x==1", HttpUtils.getRawQueryParam("scopeq=/a&idPatternq=b&q=x==1", "q"));
		assertNull(HttpUtils.getRawQueryParam("scopeQ=/a&type=T", "q"));
		assertNull(HttpUtils.getRawQueryParam(null, "q"));
	}
}
