package eu.neclab.ngsildbroker.testManager.controller;

import java.util.Enumeration;

import javax.servlet.http.HttpServletRequest;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;



@RestController
public class TestMgrController {// implements QueryHandlerInterface {

	
	 //TODO
	/**
	 * Method(POST) for "/ngsi-ld/v1/entities/" rest endpoint.
	 * 
	 * @param payload
	 *            jsonld message
	 * @return ResponseEntity object
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/test")
	@ResponseBody
	public ResponseEntity<String> createEntity(HttpServletRequest request, @RequestBody String payload) {
		//URI result = null;
		//ResponseEntity result = new ResponseEntity();
		
		System.out.println("Headers:");
		Enumeration<String> headerNames = request.getHeaderNames();
		while(headerNames.hasMoreElements()) {
			String next = headerNames.nextElement();
			System.out.println("Key: " + next);
			System.out.println("Values:");
			Enumeration<String> headers = request.getHeaders(next);
			while(headers.hasMoreElements()) {
				System.out.println(headers.nextElement());
			}
			System.out.println();
		}
		System.out.println(payload);
		String result = "Tested Successfully";
		
		System.out.println(result);
		return ResponseEntity.ok(result);
		
	}
	
}
