package eu.neclab.ngsildbroker.gateway.config;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.LoadBalancerClient;
import org.springframework.stereotype.Component;

import com.netflix.zuul.ZuulFilter;
import com.netflix.zuul.context.RequestContext;
import com.netflix.zuul.exception.ZuulException;

/**
 * To route all GET request for entities to Query-Manager service
 * 
 * @author Kailash Adhikari
 * @version 1.0
 * @date 10-Jul-2018
 */
@Component
public class PreFilter extends ZuulFilter {

	@Value("${query-manager.request-path}")
	private String REQUEST_PATH;
	@Value("${query-manager.target-service}")
	private String TARGET_SERVICE;
	@Value("${query-manager.http-method}")
	private String HTTP_METHOD;

	@Value("${entity-manager.target-service}")
	private String EM_TARGET_SERVICE;
	@Value("${entity-manager.http-method}")
	private String EM_HTTP_METHOD;

	static boolean switchQMEM = true;

	@Autowired
	private LoadBalancerClient loadBalancer;

	/**
	 * route GET requests to query-manager microservice
	 */
	@Override
	public Object run() throws ZuulException {
		RequestContext context = RequestContext.getCurrentContext();
		ServiceInstance serviceInstance;
		
		if (switchQMEM) {
			serviceInstance = loadBalancer.choose(this.TARGET_SERVICE);
		} else {
			serviceInstance = loadBalancer.choose(this.EM_TARGET_SERVICE);
		}
		try {
			if (serviceInstance != null) {
				context.setRouteHost(serviceInstance.getUri().toURL());
			} else {
				throw new IllegalStateException("Target service instance not found!");
			}
		} catch (Exception e) {
			throw new IllegalArgumentException("Couldn't get service URL!", e);
		}
		return null;
	}

	/**
	 * intercept requests and all GET requests will be handled by run() method.
	 */
	@Override
	public boolean shouldFilter() {
		RequestContext context = RequestContext.getCurrentContext();
		HttpServletRequest request = context.getRequest();
		String method = request.getMethod();
		String requestURI = request.getRequestURI();
		if (!requestURI.startsWith(REQUEST_PATH)) {
			return false;
		}
		if (HTTP_METHOD.equalsIgnoreCase(method)) {
			switchQMEM = true;
			return true;
		}
		/*
		 * if (EM_HTTP_METHOD.equalsIgnoreCase(method)) { switchQMEM = false; return
		 * true; }
		 */
		return false;
	}

	@Override
	public int filterOrder() {
		return 0;
	}

	@Override
	public String filterType() {
		return "route";
	}

}
