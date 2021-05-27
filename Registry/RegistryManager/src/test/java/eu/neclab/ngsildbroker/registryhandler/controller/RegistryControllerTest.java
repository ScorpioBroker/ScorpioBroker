package eu.neclab.ngsildbroker.registryhandler.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.net.URI;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tenant.TenantAwareDataSource;
import eu.neclab.ngsildbroker.registryhandler.repository.CSourceInfoDAO;
import eu.neclab.ngsildbroker.registryhandler.service.CSourceService;

@SpringBootTest(properties = { "spring.main.allow-bean-definition-overriding=true" })
@RunWith(SpringRunner.class)
//@WebMvcTest
@AutoConfigureMockMvc(secure = false) 
public class RegistryControllerTest {

	@Autowired
	private MockMvc mockMvc;
	@MockBean
	private CSourceService csourceService;
	
	@MockBean
	private CSourceInfoDAO cSourceInfoDAO;
	
	@MockBean
	private TenantAwareDataSource tenantAwareDataSource;

	private String payload;
	private String updatePayload;
	private String CORE_CONTEXT_URL_STR = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld";

	@Before
	public void setup() {
		// @formatter:off
		payload = "{\r\n" + "	\"id\": \"urn:ngsi-ld:ContextSourceRegistration:csr1a3458\",\r\n"
				+ "	\"type\": \"ContextSourceRegistration\",\r\n" + "	\"name\": \"NameExample\",\r\n"
				+ "	\"description\": \"DescriptionExample\",\r\n" + "	\"information\": [{\r\n"
				+ "		\"entities\": [{\r\n" + "			\"id\": \"urn:ngsi-ld:Vehicle:A456\",\r\n"
				+ "			\"type\": \"Vehicle\"\r\n" + "		}],\r\n" + "		\"properties\": [\"brandName\",\r\n"
				+ "		\"speed\"],\r\n" + "		\"relationships\": [\"isParked\"]\r\n" + "	},\r\n" + "	{\r\n"
				+ "		\"entities\": [{\r\n" + "			\"idPattern\": \".*downtown$\",\r\n"
				+ "			\"type\": \"OffStreetParking\"\r\n" + "		}]\r\n" + "	}],\r\n"
				+ "	\"endpoint\": \"http://my.csource.org:1026\",\r\n"
				+ "	\"location\": \"{ \\\"type\\\": \\\"Polygon\\\", \\\"coordinates\\\": [[[8.686752319335938,49.359122687528746],[8.742027282714844,49.3642654834877],[8.767433166503904,49.398462568451485],[8.768119812011719,49.42750021620163],[8.74305725097656,49.44781634951542],[8.669242858886719,49.43754770762113],[8.63525390625,49.41968407776289],[8.637657165527344,49.3995797187007],[8.663749694824219,49.36851347448498],[8.686752319335938,49.359122687528746]]] }\",\r\n"
				+ "	\"timestamp\": {\r\n" + "		\"start\": \"2017-11-29T14:53:15Z\"\r\n" + "	},\r\n"
				+ "	\"expires\": \"2030-11-29T14:53:15Z\",\r\n"
				+ "	\"@context\": [ \""+CORE_CONTEXT_URL_STR+"\",\r\n"
				+ "	{\r\n" + "		\"Vehicle\": \"http://example.org/vehicle/Vehicle\",\r\n"
				+ "		\"brandName\": \"http://example.org/vehicle/brandName\",\r\n"
				+ "		\"speed\": \"http://example.org/vehicle/speed\",\r\n"
				+ "		\"OffStreetParking\": \"http://example.org/parking/OffStreetParking\",\r\n"
				+ "		\"isParked\": {\r\n" + "			\"@type\": \"@id\",\r\n"
				+ "			\"@id\": \"http://example.org/common/isParked\"\r\n" + "		}\r\n" + "	}]\r\n" + "}";
		// @formatter:on
  updatePayload = "{\r\n" + "	\"id\": \"urn:ngsi-ld:ContextSourceRegistration:csr1a3458\",\r\n"
				+ "	\"type\": \"ContextSourceRegistration\",\r\n" + "	\"name\": \"NameExample\",\r\n"
				+ "	\"description\": \"DescriptionExample\",\r\n" + "	\"information\": [{\r\n"
				+ "		\"entities\": [{\r\n" + "			\"id\": \"urn:ngsi-ld:Vehicle:A456\",\r\n"
				+ "			\"type\": \"Vehicle\"\r\n" + "		}],\r\n" + "		\"properties\": [\"brandName\",\r\n"
				+ "		\"speed\",\r\n" + "\"speed1\"],\r\n" + "		\"relationships\": [\"isParked\",\r\n" + "\"isParked_New\"]\r\n" + "	},\r\n" + "	{\r\n"
				+ "		\"entities\": [{\r\n" + "			\"idPattern\": \".*downtown$\",\r\n"
				+ "			\"type\": \"OffStreetParking\"\r\n" + "		}]\r\n" + "	}],\r\n"
				+ "	\"endpoint\": \"http://my.csource.org:1026\",\r\n"
				+ "	\"location\": \"{ \\\"type\\\": \\\"Polygon\\\", \\\"coordinates\\\": [[[8.686752319335938,49.359122687528746],[8.742027282714844,49.3642654834877],[8.767433166503904,49.398462568451485],[8.768119812011719,49.42750021620163],[8.74305725097656,49.44781634951542],[8.669242858886719,49.43754770762113],[8.63525390625,49.41968407776289],[8.637657165527344,49.3995797187007],[8.663749694824219,49.36851347448498],[8.686752319335938,49.359122687528746]]] }\",\r\n"
				+ "	\"timestamp\": {\r\n" + "		\"start\": \"2017-11-29T14:53:15Z\"\r\n" + "	},\r\n"
				+ "	\"expires\": \"2030-11-29T14:53:15Z\",\r\n"
				+ "	\"@context\": [ \""+CORE_CONTEXT_URL_STR+"\",\r\n"
				+ "	{\r\n" + "		\"Vehicle\": \"http://example.org/vehicle/Vehicle\",\r\n"
				+ "		\"brandName\": \"http://example.org/vehicle/brandName\",\r\n"
				+ "		\"speed\": \"http://example.org/vehicle/speed\",\r\n"
				+ "		\"OffStreetParking\": \"http://example.org/parking/OffStreetParking\",\r\n"
				+ "		\"isParked\": {\r\n" + "			\"@type\": \"@id\",\r\n"
				+ "			\"@id\": \"http://example.org/common/isParked\"\r\n" + "		}\r\n" + "	}]\r\n" + "}";
	}

	@After
	public void teardown() {
		payload = null;
		updatePayload = null;
	}
   
	/**
	 * this method is use for the csource registration
	 */
	@Test
	public void registerCSourceTest() {
		try {
			when(csourceService.registerCSource(any(),any()))
					.thenReturn(new URI("urn:ngsi-ld:ContextSourceRegistration:csr1a3458"));
			mockMvc.perform(post("/ngsi-ld/v1/csourceRegistrations/").contentType(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
					.andExpect(status().isCreated())
					.andExpect(redirectedUrl("/ngsi-ld/v1/csourceRegistrations/urn:ngsi-ld:ContextSourceRegistration:csr1a3458"))
					.andDo(print());

			verify(csourceService, times(1)).registerCSource(any(),any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
			e.printStackTrace();
		}

	}
	
	/**
	 * this method is use for update the csource registration
	 */
	@Test
	public void updateCSourceTest() {
		try {
			mockMvc.perform(patch("/ngsi-ld/v1/csourceRegistrations/{registrationId}", "urn:ngsi-ld:ContextSourceRegistration:csr1a3458")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD).content(updatePayload))
					.andExpect(status().isNoContent()).andDo(print());
			verify(csourceService, times(1)).updateCSourceRegistration(any(),any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for delete the csource registration
	 */
	@Test
	public void deleteCsourceTest() {
		try {
			mockMvc.perform(delete("/ngsi-ld/v1/csourceRegistrations/{registrationId}", "urn:ngsi-ld:ContextSourceRegistration:csr1a3458")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)).andExpect(status().isNoContent());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
			e.printStackTrace();
		}
	}

}
