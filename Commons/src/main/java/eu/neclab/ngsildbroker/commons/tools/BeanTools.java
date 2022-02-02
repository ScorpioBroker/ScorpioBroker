package eu.neclab.ngsildbroker.commons.tools;

import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;

import reactor.netty.http.client.HttpClient;

public abstract interface BeanTools {

	public static WebClient getWebClient() {
		return WebClient.builder()
				.clientConnector(new ReactorClientHttpConnector(HttpClient.create().followRedirect(true))).build();
	}

}
