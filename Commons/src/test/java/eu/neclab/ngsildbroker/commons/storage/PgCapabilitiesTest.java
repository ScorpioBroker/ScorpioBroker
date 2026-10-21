package eu.neclab.ngsildbroker.commons.storage;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class PgCapabilitiesTest {

	@AfterEach
	public void reset() {
		PgCapabilities.reset();
	}

	@Test
	public void defaultIsPg17Behaviour() {
		assertTrue(PgCapabilities.hasJsonPathStringMethod());
	}

	@Test
	public void autoDetectsByVersion() {
		assertFalse(PgCapabilities.init(140013, "auto"));
		assertFalse(PgCapabilities.hasJsonPathStringMethod());
		assertFalse(PgCapabilities.init(160004, "auto"));
		assertTrue(PgCapabilities.init(170000, "auto"));
		assertTrue(PgCapabilities.hasJsonPathStringMethod());
		assertTrue(PgCapabilities.init(180001, "AUTO"));
	}

	@Test
	public void overrideWinsOverDetection() {
		assertTrue(PgCapabilities.init(140013, "true"));
		assertFalse(PgCapabilities.init(170002, "false"));
		assertFalse(PgCapabilities.init(170002, "FALSE"));
	}

	@Test
	public void invalidOverrideFallsBackToAuto() {
		assertFalse(PgCapabilities.init(140013, "yes"));
		assertTrue(PgCapabilities.init(170002, null));
		assertFalse(PgCapabilities.init(140013, ""));
	}
}
