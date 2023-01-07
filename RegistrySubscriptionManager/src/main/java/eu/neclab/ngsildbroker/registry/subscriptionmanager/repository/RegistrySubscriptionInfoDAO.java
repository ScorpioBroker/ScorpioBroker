package eu.neclab.ngsildbroker.registry.subscriptionmanager.repository;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Lists;

import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.DeleteSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.UpdateSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class RegistrySubscriptionInfoDAO {

	@Inject
	ClientManager clientManager;

	public Uni<RowSet<Row>> createSubscription(SubscriptionRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"INSERT INTO registry_subscriptions(subscription_id, subscription, context) VALUES ($1, $2, $3)")
					.execute(Tuple.of(request.getId(), new JsonObject(request.getPayload()),
							new JsonObject(request.getContext().serialize())));
		});
	}

	public Uni<RowSet<Row>> updateSubscription(UpdateSubscriptionRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"UPDATE registry_subscriptions SET subscription=subscription || $2, context=$3 WHERE subscription_id=$1 RETURNING subscription, context")
					.execute(Tuple.of(request.getId(), new JsonObject(request.getPayload()),
							new JsonObject(request.getContext().serialize())));
		});
	}

	public Uni<RowSet<Row>> deleteSubscription(DeleteSubscriptionRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client.preparedQuery("DELETE FROM registry_subscriptions WHERE subscription_id=$1")
					.execute(Tuple.of(request.getId()));
		});
	}

	public Uni<RowSet<Row>> getAllSubscriptions(String tenant, int limit, int offset) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT subscription, COUNT(*) FROM registry_subscriptions LIMIT $1 OFFSET $2")
					.execute(Tuple.of(limit, offset)).onFailure().retry().atMost(3);
		});
	}

	public Uni<RowSet<Row>> getSubscription(String tenant, String subscriptionId) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT subscription FROM registry_subscriptions WHERE subscription_id=$1")
					.execute(Tuple.of(subscriptionId)).onFailure().retry().atMost(3);
		});
	}

	public static void main(String[] args) throws JsonGenerationException, IOException {
		JsonLdProcessor.init("https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.6.jsonld");

		Context context = JsonLdProcessor.getCoreContextClone();
		Context test = new Context().parse(context.serialize().get("@context"), false);

		System.out.println(JsonUtils.toPrettyString(context.serialize()));
		System.out.println(JsonUtils.toPrettyString(test.serialize()));
	}

	public Uni<RowSet<Row>> getRegById(String tenant, String id) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT reg FROM csource WHERE id = $1").execute(Tuple.of(id)).onFailure()
					.retry().atMost(3);
		});
	}

	public Uni<Void> updateNotificationSuccess(String tenant, String id, String date) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("UPDATE registry_subscription SET subscription = jsonb_set(subscription, '')")
					.execute(Tuple.of(id)).onFailure().retry().atMost(3).onItem()
					.transformToUni(t -> Uni.createFrom().voidItem());
		});
	}

	public Uni<Void> updateNotificationFailure(String tenant, String id, String format) {
		// TODO Auto-generated method stub
		return null;
	}

}
