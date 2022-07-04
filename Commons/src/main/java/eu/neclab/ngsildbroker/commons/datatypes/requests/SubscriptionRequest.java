package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.Expose;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;

public class SubscriptionRequest extends BaseRequest {
	private static final String SUBSCRIPTION = "subscription";
	private static final String CONTEXT = "context";
	private static final String ACTIVE = "active";
	private static final String TYPE = "type";
	private static final String HEADERS = "headers";
	private static final String ID = "id";
	private static final String REQUEST_TYPE = "requestType";
	@Expose
	private Subscription subscription;
	@Expose
	private List<Object> context;

	private boolean active;

	private int type;

	public SubscriptionRequest() {
		// default constructor for serialization
	}

	public SubscriptionRequest(Subscription subscription, List<Object> context2,
			ArrayListMultimap<String, String> headers, int type) {
		super(headers, subscription.getId(), null, type);
		this.active = true;
		this.context = context2;
		this.subscription = subscription;
	}

	public List<Object> getContext() {
		return context;
	}

	public void setContext(List<Object> context) {
		this.context = context;
	}

	public Subscription getSubscription() {
		return subscription;
	}

	public void setSubscription(Subscription subscription) {
		this.subscription = subscription;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String toJsonString() {
		try {
			return JsonUtils.toPrettyString(toJson());
		} catch (IOException e) {
			e.printStackTrace();
			return "";
		}
	}

	public Map<String, Object> toJson() {
		Map<String, Object> top = Maps.newHashMap();
		top.put(SUBSCRIPTION, getSubscription().toJson());
		top.put(CONTEXT, getContext());
		top.put(ACTIVE, isActive());
		top.put(TYPE, getType());
		top.put(HEADERS, getHeaders(getHeaders()));
		top.put(ID, getId());
		top.put(REQUEST_TYPE, getRequestType());
		return top;

	}

	private Map<String, List<String>> getHeaders(ArrayListMultimap<String, String> headers) {
		Map<String, List<String>> top = Maps.newHashMap();
		for (Entry<String, Collection<String>> entry : headers.asMap().entrySet()) {
			String key = entry.getKey();
			List<String> value = Lists.newArrayList();
			for (String element : entry.getValue()) {
				value.add(element);
			}
			top.put(key, value);
		}
		return top;
	}

	public static SubscriptionRequest fromJsonString(String jsonString, boolean update)
			throws IOException, ResponseException {
		SubscriptionRequest result = new SubscriptionRequest();

		Map<String, Object> top = (Map<String, Object>) JsonUtils.fromString(jsonString);
		Map<String, Object> sub = null;
		for (Entry<String, Object> entry : top.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			switch (key) {
				case SUBSCRIPTION:
					sub = (Map<String, Object>) value;
					break;
				case CONTEXT:
					result.setContext((List<Object>) value);
					break;
				case ACTIVE:
					result.setActive((boolean) value);
					break;
				case TYPE:
					result.setType((int) value);
					break;
				case HEADERS:
					result.setHeaders(getMultiListHeaders((Map<String, List<String>>) value));
					break;
				case ID:
					result.setId((String) value);
					break;
				case REQUEST_TYPE:
					result.setRequestType((int) value);
					break;
				default:
					break;
			}
		}
		try {
			result.setSubscription(Subscription.expandSubscription(sub,
					JsonLdProcessor.getCoreContextClone().parse(getAtContext(result.getHeaders()), true), update));
		} catch (JsonLdError e) {
			throw new ResponseException(ErrorType.InvalidRequest, "failed to parse at context");
		}
		return result;
	}

	private static List<Object> getAtContext(ArrayListMultimap<String, String> headers) {
		return HttpUtils.parseLinkHeader(headers.get(NGSIConstants.LINK_HEADER), NGSIConstants.HEADER_REL_LDCONTEXT);
	}

	private static ArrayListMultimap<String, String> getMultiListHeaders(Map<String, List<String>> value) {
		ArrayListMultimap<String, String> result = ArrayListMultimap.create();

		for (Entry<String, List<String>> entry : value.entrySet()) {
			Iterator<String> it = entry.getValue().iterator();
			while (it.hasNext()) {
				result.put(entry.getKey(), it.next());
			}
		}
		return result;
	}

}
