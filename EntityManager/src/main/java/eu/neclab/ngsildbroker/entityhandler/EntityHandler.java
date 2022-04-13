package eu.neclab.ngsildbroker.entityhandler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

import eu.neclab.ngsildbroker.commons.securityConfig.WebSecurityConfiguration;

@SpringBootApplication
@Import(WebSecurityConfiguration.class)
public class EntityHandler {
	public static void main(String[] args) {
		System.setProperty("org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH", "true");
		SpringApplication.run(EntityHandler.class, args);
	}

}
