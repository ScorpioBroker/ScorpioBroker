
package eu.neclab.ngsildbroker.entityhandler.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;

@SpringBootTest(properties = { "spring.main.allow-bean-definition-overriding=true" })

@RunWith(SpringRunner.class)

@AutoConfigureMockMvc // (secure = false) public class
public class EntityBatchControllerTest {

	@Autowired
	private MockMvc mockMvc;

	@MockBean
	private EntityService entityService;

	private String payload;
	private String deletePayload;

	public static boolean checkEntity = false;

	@Before
	public void setup() throws Exception { //@formatter:off
  
				  payload= "[  \r\n" + "{  \r\n" +
							  "   \"id\":\"urn:ngsi-ld:Vehicle:A101\",\r\n" +
							  "   \"type\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n" +
							  "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n"
							  + "      },\r\n" + "   \"speed\":{  \r\n" +
							  "         \"type\":\"Property\",\r\n" + "         \"value\":80\r\n" +
							  "    }\r\n" + "},\r\n" + "{  \r\n" +
							  "   \"id\":\"urn:ngsi-ld:Vehicle:A102\",\r\n" +
							  "   \"type\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n" +
							  "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n"
							  + "      },\r\n" + "   \"speed\":{  \r\n" +
							  "         \"type\":\"Property\",\r\n" + "         \"value\":81\r\n" +
							  "    }\r\n" + "},\r\n" + "{  \r\n" +
							  "   \"id\":\"urn:ngsi-ld:Vehicle:A103\",\r\n" +
							  "   \"type\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n" +
							  "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n"
							  + "      },\r\n" + "   \"speed\":{  \r\n" +
							  "         \"type\":\"Property\",\r\n" + "         \"value\":82\r\n" +
							  "    }\r\n" + "}\r\n" + "]";
							  
			  deletePayload = "[  \r\n" + "\"urn:ngsi-ld:Vehicle:A101\",\r\n" +
							    "\"urn:ngsi-ld:Vehicle:A102\"\r\n" + "]"; 
  
			
			  
			  //@formatter:on 

	}

	@After
	public void tearDown() {
		payload = null;
		deletePayload = null;

	}
	// setup close

	/**
	 * this method is use for create the multiple entity
	 */
	@Test
	public void createMultipleEntityTest() {
		try {
			when(entityService.createEntry(any(), any()))
					.thenReturn(new CreateResult("urn:ngsi-ld:Vehicle:A101", true));
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/entityOperations/create").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
					.andExpect(status().isCreated());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			Assert.assertEquals(201, status);
			verify(entityService, times(3)).createEntry(any(), any());

		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for create the multiple entity but someone entity already
	 * exist.
	 */

	@Test
	public void createMultipleEntityIfEntityNotExistTest() {
		try {
			when(entityService.createEntry(any(), any())).thenReturn(new CreateResult("urn:ngsi-ld:Vehicle:A101", true))
					.thenThrow(new ResponseException(ErrorType.MultiStatus, "Multi status result"));
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/entityOperations/create").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
					.andExpect(status().isMultiStatus());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			Assert.assertEquals(207, status);
			verify(entityService, times(3)).createEntry(any(), any());

		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is validate the bad request if create the multiple entity but
	 * some entity request is not valid
	 */

	@Test
	public void createMultipleEntityBadRequestTest() {
		try {
			when(entityService.createEntry(any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/entityOperations/create").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
					.andExpect(status().isBadRequest());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			Assert.assertEquals(400, status);
			verify(entityService, times(3)).createEntry(any(), any());

		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for update the multiple entity.
	 */
	@Test
	public void updateMultipleEntityTest() {
		try {
			UpdateResult updateResult = Mockito.mock(UpdateResult.class);
			when(entityService.appendToEntry(any(), any(), any(), any())).thenReturn(updateResult);
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/entityOperations/update").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
					.andExpect(status().isNoContent());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			Assert.assertEquals(204, status);
			verify(entityService, times(3)).appendToEntry(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for create the multiple entity but someone entity not
	 * exist.
	 */
	@Test
	public void updateMultipleEntityIfEntityNotExistTest() {
		try {
			UpdateResult updateResult = Mockito.mock(UpdateResult.class);
			when(entityService.appendToEntry(any(), any(), any(), any())).thenReturn(updateResult)
					.thenThrow(new ResponseException(ErrorType.MultiStatus, "Multi status result"));
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/entityOperations/update").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
					.andExpect(status().isMultiStatus());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			Assert.assertEquals(207, status);
			verify(entityService, times(3)).appendToEntry(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is validate the bad request if update the multiple entity but
	 * some entity request is not valid
	 */
	@Test
	public void updateMultipleEntityBadRequestTest() {
		try {
			when(entityService.appendToEntry(any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/entityOperations/update").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
					.andExpect(status().isBadRequest());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			Assert.assertEquals(400, status);
			verify(entityService, times(3)).appendToEntry(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for upsert the multiple entity if all entities already
	 * exist.
	 */
	@Test
	public void upsertMultipleEntityTest() {
		try {
			when(entityService.createEntry(any(), any()))
					.thenReturn(new CreateResult("urn:ngsi-ld:Vehicle:A101", true));
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/entityOperations/upsert").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
					.andExpect(status().isCreated());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			Assert.assertEquals(201, status);
			verify(entityService, times(3)).createEntry(any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for upsert the multiple entity if someone entity not exist
	 */

	@Test
	public void upsertMultipleEntityIfEntityNotExistTest() {
		try {
			when(entityService.createEntry(any(), any())).thenReturn(new CreateResult("urn:ngsi-ld:Vehicle:A101", true))
					.thenThrow(new ResponseException(ErrorType.MultiStatus, "Multi status result"));
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/entityOperations/upsert").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
					.andExpect(status().isMultiStatus());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			Assert.assertEquals(207, status);
			verify(entityService, times(3)).createEntry(any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is validate the bad request if upsert the multiple entity but
	 * some entity request is not valid
	 */

	@Test
	public void upsertMultipleEntityBadRequestTest() {
		try {
			when(entityService.createEntry(any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/entityOperations/upsert").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
					.andExpect(status().isBadRequest());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			Assert.assertEquals(400, status);
			verify(entityService, times(3)).createEntry(any(), any());

		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for delete the multiple entity
	 */
	@Test
	public void deleteMultipleEntityTest() {
		try {
			when(entityService.deleteEntry(any(), any())).thenReturn(true);
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/entityOperations/delete").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(deletePayload))
					.andExpect(status().isNoContent());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			Assert.assertEquals(204, status);
			verify(entityService, times(2)).deleteEntry(any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for delete the multiple entity but someone entity not
	 * exist.
	 */
	@Test
	public void deleteMultipleEntityIfEntityNotExistTest() {
		try {
			when(entityService.deleteEntry(any(), any())).thenReturn(true)
					.thenThrow(new ResponseException(ErrorType.MultiStatus, "Multi status result"));
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/entityOperations/delete").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(deletePayload))
					.andExpect(status().isMultiStatus());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			Assert.assertEquals(207, status);
			verify(entityService, times(2)).deleteEntry(any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

}
