package eu.neclab.ngsildbroker.entityhandler.controller;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import eu.neclab.ngsildbroker.commons.datatypes.BatchResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;

@RestController
@RequestMapping("/ngsi-ld/v1/entityOperations")
public class EntityBatchController {
	
	@Autowired
	EntityService entityService;
	
	@Autowired
	@Qualifier("emconRes")
	ContextResolverBasic contextResolver;

	
	HttpUtils httpUtils;
	@PostConstruct
	private void setup() {
		 httpUtils = HttpUtils.getInstance(contextResolver);
	}
	@PostMapping("/create")
	public ResponseEntity<byte[]> createMultiple(HttpServletRequest request,
			@RequestBody String payload) throws ResponseException{
		try {
			HttpUtils.doPreflightCheck(request, payload);
			String resolved = httpUtils.expandPayload(request, payload);
			BatchResult result = entityService.createMultipleMessage(resolved);
			return generateBatchResultReply(result);
		} catch (MalformedURLException | UnsupportedEncodingException e) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
	}
	
	private ResponseEntity<byte[]> generateBatchResultReply(BatchResult result) {
//		HttpStatus status = HttpStatus.MULTI_STATUS;
//		if(result.getFails().isEmpty()) {
//			status = HttpStatus.OK;
//		}
//		if(result.getSuccess().isEmpty()) {
//			status = HttpStatus.BAD_REQUEST;
//		}
		HttpStatus status = HttpStatus.OK;
		return httpUtils.generateReply(DataSerializer.toJson(result), null, status);
	}
	@PostMapping("/upsert")
	public ResponseEntity<byte[]> upsertMultiple(HttpServletRequest request,
			@RequestBody String payload) throws ResponseException{
		try {
			HttpUtils.doPreflightCheck(request, payload);
			String resolved = httpUtils.expandPayload(request, payload);
			BatchResult result = entityService.upsertMultipleMessage(resolved);
			return generateBatchResultReply(result);
		} catch (MalformedURLException | UnsupportedEncodingException e) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
	}
	
	@PostMapping("/update")
	public ResponseEntity<byte[]> updateMultiple(HttpServletRequest request,
			@RequestBody String payload) throws ResponseException{
		try {
			HttpUtils.doPreflightCheck(request, payload);
			String resolved = httpUtils.expandPayload(request, payload);
			BatchResult result = entityService.updateMultipleMessage(resolved);
			return generateBatchResultReply(result);
		} catch (MalformedURLException | UnsupportedEncodingException e) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
	}
	
	@PostMapping("/delete")
	public ResponseEntity<byte[]> deleteMultiple(HttpServletRequest request,
			@RequestBody String payload) throws ResponseException{
		try {
//			String resolved = httpUtils.expandPayload(request, payload);
			//it's an array of uris which is not json-ld so no expanding here  
			BatchResult result = entityService.deleteMultipleMessage(payload);
			return generateBatchResultReply(result);
		} catch (ResponseException e) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
	}

}
