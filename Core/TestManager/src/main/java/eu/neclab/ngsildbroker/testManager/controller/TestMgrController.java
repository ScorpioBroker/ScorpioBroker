package eu.neclab.ngsildbroker.testManager.controller;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestMgrController {// implements QueryHandlerInterface {

	// TODO
	/**
	 * Method(POST) for "/ngsi-ld/v1/entities/" rest endpoint.
	 * 
	 * @param payload jsonld message
	 * @return ResponseEntity object
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/test")
	@ResponseBody
	public ResponseEntity<String> createEntity(ServerHttpRequest request, @RequestBody String payload) {
		// URI result = null;
		// ResponseEntity result = new ResponseEntity();

		System.out.println("Headers:");
		Iterator<Entry<String, List<String>>> headerNames = request.getHeaders().entrySet().iterator();
		while (headerNames.hasNext()) {
			Entry<String, List<String>> next = headerNames.next();
			System.out.println("Key: " + next.getKey());
			System.out.println("Values:");
			List<String> headers = next.getValue();
			for (String entry : headers) {
				System.out.println(entry);
			}
			System.out.println();
		}
		System.out.println(payload);
		String result = "Tested Successfully";

		System.out.println(result);
		return ResponseEntity.ok(result);

	}

}
