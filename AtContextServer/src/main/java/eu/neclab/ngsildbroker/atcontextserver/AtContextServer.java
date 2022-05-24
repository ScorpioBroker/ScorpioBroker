package eu.neclab.ngsildbroker.atcontextserver;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;

@QuarkusMain
//@Import(WebSecurityConfiguration.class)
public class AtContextServer {

	public static void main(String[] args) {
		Quarkus.run(args);
	}

}
