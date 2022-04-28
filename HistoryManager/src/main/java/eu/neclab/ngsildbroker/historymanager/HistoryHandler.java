package eu.neclab.ngsildbroker.historymanager;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;

@QuarkusMain
//@Import(WebSecurityConfiguration.class)
public class HistoryHandler {
	public static void main(String[] args) {
		System.setProperty("org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH", "true");
		Quarkus.run(args);
	}
}
