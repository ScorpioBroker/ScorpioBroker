package eu.neclab.ngsildbroker.historymanager.controller;

import org.junit.jupiter.api.TestMethodOrder;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import org.junit.jupiter.api.Test;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;

@QuarkusTest
@TestMethodOrder(OrderAnnotation.class)
@TestProfile(CustomProfile.class)
public class EntityOperationsHistoryControllerTest {

	private String createpayload;
	private String deletePayload;
	private String qPayload;
	private String badRequestPayload;
	private String badRequestPayloadIdNotProvied;
	private String updatePayload;
	private String payloadNotFoundEntity;

	@BeforeEach
	public void setup() throws Exception { //@formatter:off
  
				  createpayload= "[  \r\n" + "{  \r\n" +
							  "   \"id\":\"urn:ngsi-ld:Vehicle:0a0\",\r\n" +
							  "   \"type\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n" +
							  "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n"
							  + "      },\r\n" + "   \"speed\":{  \r\n" +
							  "         \"type\":\"Property\",\r\n" + "         \"value\":80\r\n" +
							  "    }\r\n" + "},\r\n" + "{  \r\n" +
							  "   \"id\":\"urn:ngsi-ld:Vehicle:0a1\",\r\n" +
							  "   \"type\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n" +
							  "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n"
							  + "      },\r\n" + "   \"speed\":{  \r\n" +
							  "         \"type\":\"Property\",\r\n" + "         \"value\":81\r\n" +
							  "    }\r\n" + "},\r\n" + "{  \r\n" +
							  "   \"id\":\"urn:ngsi-ld:Vehicle:0a2\",\r\n" +
							  "   \"type\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n" +
							  "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n"
							  + "      },\r\n" + "   \"speed\":{  \r\n" +
							  "         \"type\":\"Property\",\r\n" + "         \"value\":82\r\n" +
							  "    }\r\n" + "}\r\n" + "]";
				  
				  
				  badRequestPayload = "[  \r\n" + "{  \r\n" +
						  "   \"id\":\"urn:ngsi-ld:Vehicle:0a0\",\r\n" +
						  "   \"type123\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n" +
						  "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n"
						  + "      },\r\n" + "   \"speed\":{  \r\n" +
						  "         \"type\":\"Property\",\r\n" + "         \"value\":80\r\n" +
						  "    }\r\n" + "},\r\n" + "{  \r\n" +
						  "   \"id\":\"urn:ngsi-ld:Vehicle:0a1\",\r\n" +
						  "   \"type123\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n" +
						  "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n"
						  + "      },\r\n" + "   \"speed\":{  \r\n" +
						  "         \"type\":\"Property\",\r\n" + "         \"value\":81\r\n" +
						  "    }\r\n" + "},\r\n" + "{  \r\n" +
						  "   \"id\":\"urn:ngsi-ld:Vehicle:0a2\",\r\n" +
						  "   \"type123\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n" +
						  "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n"
						  + "      },\r\n" + "   \"speed\":{  \r\n" +
						  "         \"type\":\"Property\",\r\n" + "         \"value\":82\r\n" +
						  "    }\r\n" + "}\r\n" + "]"; 
				  
				  
				 
				  
				  badRequestPayloadIdNotProvied="[\r\n"
				  		+ "    {\r\n"
				  		+ "        \"\": \"\",\r\n"
				  		+ "        \"type\": \"Room\",\r\n"
				  		+ "        \"isPartOf\": {\r\n"
				  		+ "            \"type\": \"Relationship\",\r\n"
				  		+ "            \"object\": \"smartcity:houses:house99\"\r\n"
				  		+ "        }\r\n"
				  		+ "    } \r\n"
				  		+ "]";
				  
				  updatePayload= "[  \r\n" + "{  \r\n" +
						  "   \"id\":\"urn:ngsi-ld:Vehicle:0a0\",\r\n" +
						  "   \"type\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n" +
						  "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n"
						  + "      },\r\n" + "   \"speed\":{  \r\n" +
						  "         \"type\":\"Property\",\r\n" + "         \"value\":80\r\n" +
						  "    }\r\n" + "},\r\n" + "{  \r\n" +
						  "   \"id\":\"urn:ngsi-ld:Vehicle:0a1\",\r\n" +
						  "   \"type\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n" +
						  "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n"
						  + "      },\r\n" + "   \"speed\":{  \r\n" +
						  "         \"type\":\"Property\",\r\n" + "         \"value\":81\r\n" +
						  "    }\r\n" + "},\r\n" + "{  \r\n" +
						  "   \"id\":\"urn:ngsi-ld:Vehicle:0a2\",\r\n" +
						  "   \"type\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n" +
						  "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n"
						  + "      },\r\n" + "   \"speed\":{  \r\n" +
						  "         \"type\":\"Property\",\r\n" + "         \"value\":82\r\n" +
						  "    }\r\n" + "}\r\n" + "]";
							  
				  
				  payloadNotFoundEntity="[\r\n"
				  		+ "    {\r\n"
				  		+ "        \"id\": \"house99:smartrooms:l101\",\r\n"
				  		+ "        \"type\": \"Room\" \r\n"
				  		+ "    }\r\n"
				  		+ "]";  
				  
				  
			  deletePayload = "[  \r\n" + "\"urn:ngsi-ld:Vehicle:0a0\",\r\n" +
							    "\"urn:ngsi-ld:Vehicle:0a1\",\r\n" + 
							    "\"urn:ngsi-ld:Vehicle:0a2\"\r\n" +"]"; 
  
			qPayload ="{\r\n"
					+ " \"type\": \"Query\",\r\n"
					+ "  \"entities\": [\r\n"
					+ "    {\r\n"
					+ "   \"type\":\"Vehicle\"\r\n"
					+ "   }]\r\n"
					+ "  \r\n"
					+ "}";
			
		 
			
		 
			 

	}

	@AfterEach
	public void tearDown() {
		  createpayload=null;
		  deletePayload=null;
		  qPayload=null;
	 	  badRequestPayload=null;
		  badRequestPayloadIdNotProvied=null;
		  updatePayload=null;
		 payloadNotFoundEntity=null;
	 

	}
	// setup close

	/**
	 * this method is use for create the multiple entity
	 */
 
	@Test
	@Order(1)
	public void createMultipleEntityTest() {
	   try {
			ExtractableResponse<Response> response = given().body(createpayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/temporal/entityOperations/create").then().statusCode(Status.CREATED.getStatusCode()).statusCode(201)
					.extract();
			int statsCode = response.statusCode();
 			assertEquals(201, statsCode);
		}catch(Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}
	
	
	/**
	 * this method is validate the bad request if create the multiple entity but
	 * some entity request is not valid
	 */

	@Test
	@Order(2)
	public void createMultipleEntityBadRequestTest() {
		
		try {    
	        ExtractableResponse<Response> response =given()
	                  .body(badRequestPayload)
	                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
	                 .header(HttpHeaders.ACCEPT,AppConstants.NGB_APPLICATION_JSONLD)
	                .when()
	                .post("/ngsi-ld/v1/temporal/entityOperations/create")
	                .then()
	                   .statusCode(Status.BAD_REQUEST.getStatusCode())
	                   .statusCode(400).extract();
	            int statusCode = response.statusCode();
	            assertEquals(400, statusCode);
	            
	    }catch(Exception e) {
	        e.printStackTrace();
	    }

	}
	
	/**
	 * this method is validate the bad request if create the multiple entity but
	 * some entity @id  not provided
	 */
	@Test
	@Order(3)
	public void createMultipleEntityBadRequestIDNotProviedTest() {
		
		try {    
	        ExtractableResponse<Response> response =given()
	                  .body(badRequestPayloadIdNotProvied)
	                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
	                 .header(HttpHeaders.ACCEPT,AppConstants.NGB_APPLICATION_JSONLD)
	                .when()
	                .post("/ngsi-ld/v1/temporal/entityOperations/create")
	                .then()
	                   .statusCode(Status.BAD_REQUEST.getStatusCode())
	                   .statusCode(400).extract();
	            int statusCode = response.statusCode();
	            assertEquals(400, statusCode);
	            
	    }catch(Exception e) {
	        e.printStackTrace();
	    }

	}
	
	/**
	 * this method is use for update the multiple entity.
	 */
	
	@Test
	@Order(4)
	public void updateMultipleEntityTest() {
		
		try {    
	        ExtractableResponse<Response> response = given()
	                  .body(updatePayload)
	                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
	                 .header(HttpHeaders.ACCEPT,AppConstants.NGB_APPLICATION_JSONLD)
	                .when()
	                .post("/ngsi-ld/v1/temporal/entityOperations/update")
	                .then()
	                   .statusCode(Status.NO_CONTENT.getStatusCode())
	                   .statusCode(204).extract();
	            int statusCode = response.statusCode();
	            assertEquals(204, statusCode);
	            
	    }catch(Exception e) {
	        e.printStackTrace();
	    }

	}
	
	/**
	 * this method is use to verify Id Not Provided Bad Request in Payload.
	 */
	@Test
	@Order(5)
	public void updateMultipleEntityIdNotProviedBadRequestTest() {
		
		try {    
	        ExtractableResponse<Response> response = given()
	                  .body(badRequestPayloadIdNotProvied)
	                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
	                 .header(HttpHeaders.ACCEPT,AppConstants.NGB_APPLICATION_JSONLD)
	                .when()
	                .post("/ngsi-ld/v1/temporal/entityOperations/update")
	                .then()
	                   .statusCode(Status.BAD_REQUEST.getStatusCode())
	                   .statusCode(400).extract();
	          
	            assertEquals(400, response.statusCode());
	            
	    }catch(Exception e) {
	    	Assertions.fail();
	        e.printStackTrace();
	    }

	}
    
	/**
	 * this method is use to verify Multiple Entity Id Not Found Test.
	 */
	@Test
	@Order(6)
	public void updateMultipleEntityNotFoundTest() {
		
		try {    
	        ExtractableResponse<Response> response = given()
	                  .body(payloadNotFoundEntity)
	                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
	                 .header(HttpHeaders.ACCEPT,AppConstants.NGB_APPLICATION_JSONLD)
	                .when()
	                .post("/ngsi-ld/v1/temporal/entityOperations/update")
	                .then()
	                   .statusCode(Status.NOT_FOUND.getStatusCode())
	                   .statusCode(404).extract();
	          
	            assertEquals(404, response.statusCode());
	            
	    }catch(Exception e) {
	    	Assertions.fail();
	        e.printStackTrace();
	    }
	}
	
	 
	/**
	 * this method is use for upsert the multiple entity if all entities already
	 * exist.
	 */
	@Test
	@Order(7)
	public void upsertMultipleEntityTest() {
		
		try {    
	        ExtractableResponse<Response> response = given()
	                  .body(createpayload)
	                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
	                 .header(HttpHeaders.ACCEPT,AppConstants.NGB_APPLICATION_JSONLD)
	                .when()
	                .post("/ngsi-ld/v1/temporal/entityOperations/upsert")
	                .then()
	                   .statusCode(Status.NO_CONTENT.getStatusCode())
	                   .statusCode(204).extract();
	            
	            assertEquals(204, response.statusCode());
	            
	    }catch(Exception e) {
	        Assertions.fail();
	    	e.printStackTrace();
	    }
	
	}
	
	
	/**
	 * this method is validate the bad request if create the multiple entity but
	 * some entity @id  not provided
	 */
	@Test
	@Order(8)
	public void upsertMultipleEntityBadRequestIDNotProviedTest() {
    	try {    
	        ExtractableResponse<Response> response =given()
	                  .body(badRequestPayloadIdNotProvied)
	                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
	                 .header(HttpHeaders.ACCEPT,AppConstants.NGB_APPLICATION_JSONLD)
	                .when()
	                .post("/ngsi-ld/v1/temporal/entityOperations/upsert")
	                .then()
	                   .statusCode(Status.BAD_REQUEST.getStatusCode())
	                   .statusCode(400).extract();
	            int statusCode = response.statusCode();
	            assertEquals(400, statusCode);
	            
	    }catch(Exception e) {
	        e.printStackTrace();
	    }

	}
	
	/**
	 * this method is validate the postQuery method 
	 *  
	 */
	@Test
	@Order(9)
	public void postQueryTest() {
    	try {
			ExtractableResponse<Response> response = given().body(qPayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/temporal/entityOperations/query").then()
					.statusCode(Status.OK.getStatusCode()).statusCode(200).extract();
			assertEquals(200,  response.statusCode());
		}catch(Exception e) {
			Assertions.fail();
		}
	}
	 
	
	
	/**
	 * this method is validate the delete multiple entities
	 *  
	 */
	@Test
	@Order(10)
	public void deleteMultipleEntityTest() {
	   try {
			ExtractableResponse<Response> response = given().body(deletePayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/temporal/entityOperations/delete").then().statusCode(Status.NO_CONTENT.getStatusCode()).statusCode(204)
					.extract();
			int statsCode = response.statusCode();
 			assertEquals(204, statsCode);
		}catch(Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}
	
	/**
	 * this method is validate the delete multiple entities BadRequest
	 *  
	 */
	@Test
	@Order(11)
	public void deteleMultipleEntityBadRequestTest() {
	   try {
			ExtractableResponse<Response> response = given().body(deletePayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/temporal/entityOperations/delete").then().statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400)
					.extract();
			int statsCode = response.statusCode();
 			assertEquals(400, statsCode);
		}catch(Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}
   
 
	
}
