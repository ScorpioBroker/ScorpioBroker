package eu.neclab.ngsildbroker.registryhandler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

import eu.neclab.ngsildbroker.commons.securityConfig.WebSecurityConfiguration;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;

@SpringBootApplication
@Import({ WebSecurityConfiguration.class, MicroServiceUtils.class })
public class RegistryHandler {

	public static void main(String[] args) {
		System.setProperty("org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH", "true");
		SpringApplication.run(RegistryHandler.class, args);
	}

}
