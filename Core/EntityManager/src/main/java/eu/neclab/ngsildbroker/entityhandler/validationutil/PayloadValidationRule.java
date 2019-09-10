package eu.neclab.ngsildbroker.entityhandler.validationutil;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

@Component
public class PayloadValidationRule implements ValidationRules{

	/**
	 * expecting a unresolved  raw payload
	 */
	public boolean validateEntity(String payload,HttpServletRequest request) throws ResponseException {
		if (payload == null) {
			throw new ResponseException(ErrorType.BadRequestData,"Empty request payload");
		}
		JsonNode json = null;
		try {
			json = objectMapper.readTree(payload);
			if (json.isNull()) {
				throw new ResponseException(ErrorType.UnprocessableEntity);
			}
			isValidContentType(json,request.getContentType());
			isAttrsContainsForbiddenCharacters(json);
		} catch (JsonParseException e) {
			throw new ResponseException(ErrorType.BadRequestData);
		} catch (IOException e) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		return true;
	}
	
	//check for forbidden characters in payload
	private boolean isAttrsContainsForbiddenCharacters(JsonNode payload) throws ResponseException{
		//TODO : need to generate regex for all forbidden characters in NGB
		Pattern p = Pattern.compile(".[<\"'=;()>?*]", Pattern.CASE_INSENSITIVE);
		Iterator<String> fieldNames=payload.fieldNames();
		while(fieldNames.hasNext()) {
			Matcher m = p.matcher(fieldNames.next());
			if(m.find()) {
				throw new ResponseException(ErrorType.BadRequestData,"Forbidden characters in payload body");
			}
		}
		return true;
	}
	
	//check for json/@context OR json-LD/link validation in payload
	private boolean isValidContentType(JsonNode payload,String contentType) throws ResponseException {
		if(contentType.equalsIgnoreCase(AppConstants.NGB_APPLICATION_JSON)) {
			if(payload.get("@context")!=null) {
				throw new ResponseException(ErrorType.BadRequestData,"@context is provided in a JSON payload");
			}
		}else if(contentType.equalsIgnoreCase(AppConstants.NGB_APPLICATION_JSONLD)) {
			if(payload.get("@context")==null) {
				throw new ResponseException(ErrorType.BadRequestData,"No @context is provided in a JSON-LD payload");
			}
			//link header validation
		}else {
			throw new ResponseException(ErrorType.UnsupportedMediaType,"Media Type ( "+contentType+") is not supported in NGB");
		}
		return true;
	}

	@Override
	public boolean validateEntity(Entity entity, HttpServletRequest request) throws ResponseException {
		return true;
	}
	

}
