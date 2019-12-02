package eu.neclab.ngsildbroker.storagemanager.controller;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
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

import eu.neclab.ngsildbroker.storagemanager.repository.EntityStorageReaderDAO;

@RestController
@RequestMapping("/scorpio/v1/info")
public class StorageController {
	@Autowired
	EntityStorageReaderDAO storageReaderDao;

	
	@GetMapping("/")
	public ResponseEntity<Object> getDefault() {
		return ResponseEntity.status(HttpStatus.ACCEPTED).body("available subresources:\n/types");
	}
	

	@GetMapping(path = "/types")
	public ResponseEntity<Object> getTypes(HttpServletRequest request) {
		List<String> types = storageReaderDao.getAllTypes();
		if(types == null) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("failed to get available types");
		}
		return ResponseEntity.status(HttpStatus.ACCEPTED).body(DataSerializer.toJson(types));
	}
	
}
