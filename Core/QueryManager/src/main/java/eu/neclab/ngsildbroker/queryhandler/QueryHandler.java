package eu.neclab.ngsildbroker.queryhandler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import eu.neclab.ngsildbroker.commons.securityConfig.WebSecurityConfiguration;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;

@SpringBootApplication
@Import({ WebSecurityConfiguration.class, MicroServiceUtils.class })
public class QueryHandler {

	@Autowired
	private MicroServiceUtils microServiceUtils;

	public static void main(String[] args) {
		System.setProperty("org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH", "true");
		SpringApplication.run(QueryHandler.class, args);
	}

}
