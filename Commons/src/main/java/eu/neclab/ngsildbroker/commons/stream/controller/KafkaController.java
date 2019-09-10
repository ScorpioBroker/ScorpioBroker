package eu.neclab.ngsildbroker.commons.stream.controller;

import java.util.Collections;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaService;

@RestController()
@RequestMapping("/kafka")
public class KafkaController {
	
	@Autowired
	KafkaService kafkaService;
	@Autowired
	KafkaOps ops;
	
	@PostMapping(value="/createmessage")
	public ResponseEntity<String> createTopicMessage(@RequestBody String request) {
		String result = kafkaService.createMessage(request);
		if("DUPLICATE".equalsIgnoreCase(result)) {
			return new ResponseEntity<String>("Already Exists.",HttpStatus.OK);
		}else if("CREATED".equalsIgnoreCase(result)) {
			return new ResponseEntity<String>("Created.",HttpStatus.OK);
		}else {
			return new ResponseEntity<String>("Internal Server Error.",HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
	
	@DeleteMapping(value="/deletemessage")
	public ResponseEntity<String> deleteTopicMessage(@RequestBody String request){
		String result= kafkaService.deleteTopicMessage(request);
		if("ERROR".equalsIgnoreCase(result)) {
			return new ResponseEntity<String>("Internal Server Error",HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<String>("Deleted. ",HttpStatus.OK);
	}
	
	@GetMapping(value="/create")
	public ResponseEntity<String> createTopic(@RequestParam("topicname") String topicName){
		ops.createTopic(topicName);
		return new ResponseEntity<String>("Created",HttpStatus.OK);
	}
	
	@DeleteMapping(value="/delete")
	public ResponseEntity<String> deleteTopic(@RequestParam("topicname") String topicName) throws Exception{
		ops.deleteTopic(Collections.singletonList(topicName));
		return new ResponseEntity<String>("Deleted",HttpStatus.OK);
	}
	
	@GetMapping("/getalltopics")
	public ResponseEntity<Set<String>> getAllTopics() throws Exception{
		Set<String> list=ops.getTopics();
		return new ResponseEntity<Set<String>>(list,HttpStatus.OK);
	}
	
}
