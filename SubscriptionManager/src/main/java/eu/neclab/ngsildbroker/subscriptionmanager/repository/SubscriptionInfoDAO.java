package eu.neclab.ngsildbroker.subscriptionmanager.repository;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Lists;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.DeleteSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.UpdateSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


@ApplicationScoped
public class SubscriptionInfoDAO {
    @Inject
    Vertx vertx;
    WebClient webClient;
    @Inject
    ClientManager clientManager;

    @PostConstruct
    void setup() {
        webClient = WebClient.create(vertx);
    }
    public static void main(String[] args) throws IOException {
        JsonLdProcessor.init("https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.6.jsonld");

        Context context = JsonLdProcessor.getCoreContextClone();
        Context test = new Context().parse(context.serialize().get("@context"), false);

        System.out.println(JsonUtils.toPrettyString(context.serialize()));
        System.out.println(JsonUtils.toPrettyString(test.serialize()));
    }
    public Uni<RowSet<Row>> createSubscription(SubscriptionRequest request) {
        return clientManager.getClient(request.getTenant(), true).onItem()
                .transformToUni(client ->
                        webClient.postAbs("http://localhost:9090/ngsi-ld/v1/jsonldContexts/createimplicitly/")
                                .sendJsonObject(new JsonObject(request.getContext().serialize()))
                                .onItemOrFailure().transformToUni((item, failure) -> {
                                    if (failure != null) return Uni.createFrom().failure(new Throwable("Something went wrong"));
                                    String contextId = item.bodyAsString();
                                    return client.preparedQuery(
                                                    "INSERT INTO subscriptions(subscription_id, subscription, context) VALUES ($1, $2, $3)")
                                            .execute(Tuple.of(request.getId(), new JsonObject(request.getPayload()),
                                                    contextId));
                                }));
    }

    public Uni<Tuple2<Map<String, Object>,  Object>> updateSubscription(UpdateSubscriptionRequest request) {
        return clientManager.getClient(request.getTenant(), false).onItem()
                .transformToUni(client ->
                        webClient.postAbs("http://localhost:9090/ngsi-ld/v1/jsonldContexts/createimplicitly/")
                                .sendJsonObject(new JsonObject(request.getContext().serialize()))
                                .onItemOrFailure().transformToUni((item, failure) -> {
                                    if (failure != null) throw new RuntimeException();
                                    String contextId = item.bodyAsString();
                                    return client.preparedQuery(
                                                    "UPDATE subscriptions SET subscription=subscription || $2, context=$3 WHERE subscription_id=$1 RETURNING subscription")
                                            .execute(Tuple.of(request.getId(), new JsonObject(request.getPayload()),
                                                    contextId)).onItem().transform(i-> Tuple2.of(request.getPayload(),request.getContext().serialize().get("@context")));
                                }));
    }

    public Uni<RowSet<Row>> deleteSubscription(DeleteSubscriptionRequest request) {
        return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> client.preparedQuery("DELETE FROM subscriptions WHERE subscription_id=$1")
                .execute(Tuple.of(request.getId())));
    }

    public Uni<RowSet<Row>> getAllSubscriptions(String tenant, int limit, int offset) {
        return clientManager.getClient(tenant, false).onItem().transformToUni(client -> client.preparedQuery("SELECT subscription FROM subscriptions LIMIT $1 OFFSET $2")
                .execute(Tuple.of(limit, offset)).onFailure().retry().atMost(3));
    }

    public Uni<RowSet<Row>> getSubscription(String tenant, String subscriptionId) {
        return clientManager.getClient(tenant, false).onItem().transformToUni(client -> client.preparedQuery("SELECT subscription FROM subscriptions WHERE subscription_id=$1")
                .execute(Tuple.of(subscriptionId)).onFailure().retry().atMost(3));
    }

    public Uni<RowSet<Row>> getRegById(String tenant, String id) {
        return clientManager.getClient(tenant, false).onItem().transformToUni(client -> client.preparedQuery("SELECT reg FROM csource WHERE id = $1").execute(Tuple.of(id)).onFailure()
                .retry().atMost(3));
    }

    public Uni<Void> updateNotificationSuccess(String tenant, String id, String date) {
        return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
           return client
                    .preparedQuery("UPDATE subscriptions SET subscription = subscription || ('{\""
                            + NGSIConstants.NGSI_LD_TIMES_SENT + "\": [{\"" + NGSIConstants.JSON_LD_VALUE
                            + "\": '|| (subscription#>>'{" + NGSIConstants.NGSI_LD_TIMES_SENT + ",0, "
                            + NGSIConstants.JSON_LD_VALUE + "}')::integer + 1 ||'}],\""
                            + NGSIConstants.NGSI_LD_LAST_SUCCESS + "\": [{\"" + NGSIConstants.JSON_LD_TYPE + "\": \""
                            + NGSIConstants.NGSI_LD_DATE_TIME + "\", \"" + NGSIConstants.JSON_LD_VALUE + "\": \"$1\"}],\""
                            + NGSIConstants.NGSI_LD_LAST_NOTIFICATION + "\": [{\"" + NGSIConstants.JSON_LD_TYPE
                            + "\": \"" + NGSIConstants.NGSI_LD_DATE_TIME + "\", \"" + NGSIConstants.JSON_LD_VALUE
                            + "\": \"$1\"}]}')::jsonb WHERE subscription_id=$2")
                    .execute(Tuple.of(date, id)).onFailure().retry().atMost(3).onItem()
                    .transformToUni(t -> Uni.createFrom().voidItem());
        });
    }

    public Uni<Void> updateNotificationFailure(String tenant, String id, String date) {
        return clientManager.getClient(tenant, false).onItem().transformToUni(client -> client
                .preparedQuery("UPDATE subscriptions SET subscription = subscription || ('{\""
                        + NGSIConstants.NGSI_LD_TIMES_FAILED + "\": [{\"" + NGSIConstants.JSON_LD_VALUE
                        + "\": '|| (subscription#>>'{" + NGSIConstants.NGSI_LD_TIMES_FAILED + ",0, "
                        + NGSIConstants.JSON_LD_VALUE + "}')::integer + 1 ||'}],\""
                        + NGSIConstants.NGSI_LD_LAST_FAILURE + "\": [{\"" + NGSIConstants.JSON_LD_TYPE + "\": \""
                        + NGSIConstants.NGSI_LD_DATE_TIME + "\", \"" + NGSIConstants.JSON_LD_VALUE + "\": \"$1\"}],\""
                        + NGSIConstants.NGSI_LD_LAST_NOTIFICATION + "\": [{\"" + NGSIConstants.JSON_LD_TYPE
                        + "\": \"" + NGSIConstants.NGSI_LD_DATE_TIME + "\", \"" + NGSIConstants.JSON_LD_VALUE
                        + "\": \"$1\"}]}')::jsonb WHERE subscription_id=$2")
                .execute(Tuple.of(date, id)).onFailure().retry().atMost(3).onItem()
                .transformToUni(t -> Uni.createFrom().voidItem()));
    }

    public Uni<List<Tuple3<String, Map<String, Object>, Map<String, Object>>>> loadSubscriptions() {
        return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
            return client.preparedQuery("select tenant_id from tenant").execute().onItem().transformToUni(rows -> {
                List<Uni<RowSet<Row>>> unis = Lists.newArrayList();
                rows.forEach(row -> {
                    unis.add(clientManager.getClient(row.getString(0), false).onItem().transformToUni(tenantClient -> tenantClient.preparedQuery(
                                    "SELECT '" + row.getString(0) + "', subscriptions.subscription, context FROM subscriptions")
                            .execute()));
                });
                unis.add(clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem()
                        .transformToUni(tenantClient -> tenantClient.preparedQuery("SELECT '" + AppConstants.INTERNAL_NULL_KEY
                                + "', subscriptions.subscription, context FROM subscriptions").execute()));

                return Uni.combine().all().unis(unis).combinedWith(list -> {
                    List<Tuple3<String, Map<String, Object>, Map<String, Object>>> result = new ArrayList<>();
                    return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(pgPool->{
                       return  pgPool.preparedQuery("select jsonb_object_agg(id,body) as col from public.contexts").execute()
                               .onItem().transform(rows1 -> {
                                   JsonObject jsonContexts = rows1.iterator().next().getJsonObject(0);
                                   Map<String, Object> mapContexts;
                                   if(jsonContexts != null) mapContexts = jsonContexts.getMap();
                                   else return result;
                                   for (Object obj : list) {
                                @SuppressWarnings("unchecked")
                                RowSet<Row> rowset = (RowSet<Row>) obj;
                                rowset.forEach(row -> result.add(Tuple3.of(row.getString(0),
                                        row.getJsonObject(1).getMap(),
                                        (Map<String, Object>) mapContexts.get(row.getString(2))))
                                );
                            }
                            return result;
                        });

                    });

                }).onItem().transformToUni(x->x);
            });
        });

    }

    public Uni<RowSet<Row>> getInitialNotificationData(SubscriptionRequest subscriptionRequest) {
        return clientManager.getClient(subscriptionRequest.getTenant(), false).onItem().transformToUni(client -> {
            List<Object> tupleItems = Lists.newArrayList();
            String sql = "with a as (select cs_id from contextsourceinformation WHERE ";
            boolean sqlAdded = false;
            int dollar = 1;
            Subscription subscription = subscriptionRequest.getSubscription();
            Iterator<EntityInfo> it = subscription.getEntities().iterator();
            while (it.hasNext()) {
                EntityInfo entityInformation = it.next();
                sql += "(";
                if (entityInformation.getId() != null) {
                    sql += "((e_id is null or e_id  = $" + dollar + ") and (e_id_p is null or e_id_p ~ $" + dollar
                            + "))";
                    dollar++;
                    tupleItems.add(entityInformation.getId().toString());
                    if (entityInformation.getType() != null) {
                        sql += " and ";
                    }
                } else if (entityInformation.getIdPattern() != null) {
                    sql += "((e_id is null or $" + dollar + " ~ e_id) and (e_id_p is null or e_id_p = $" + dollar
                            + "))";
                    dollar++;
                    tupleItems.add(entityInformation.getIdPattern());
                    if (entityInformation.getType() != null) {
                        sql += " and ";
                    }
                }
                if (entityInformation.getType() != null) {
                    sql += "(e_type is null or e_type in $" + dollar + ")";
                    dollar++;
                    tupleItems.add(entityInformation.getType());
                }
                sql += ")";
                if (it.hasNext()) {
                    sql += " and ";
                }
                sqlAdded = true;
            }

            if (subscription.getAttributeNames() != null) {
                if (sqlAdded) {
                    sql += " and ";
                }
                sql += "(e_prop is null or e_prop in $" + dollar + ") and (e_rel is null or e_rel in $" + dollar + ")";
                tupleItems.add(subscription.getAttributeNames());
                dollar++;
                sqlAdded = true;
            }

            if (subscription.getLdGeoQuery() != null) {
                if (sqlAdded) {
                    sql += " and ";
                }
                try {
                    Tuple2<StringBuilder, Integer> tmp = subscription.getLdGeoQuery().getGeoSQLQuery(tupleItems, dollar,
                            "i_location");
                    sql += tmp.getItem1().toString();
                    dollar = tmp.getItem2();
                    sqlAdded = true;
                } catch (ResponseException e) {
                    return Uni.createFrom().failure(e);
                }
            }

            if (subscription.getScopeQuery() != null) {
                if (sqlAdded) {
                    sql += " and ";
                }
                sql += "(scopes IS NULL OR ";
                ScopeQueryTerm current = subscription.getScopeQuery();
                while (current != null) {
                    sql += " matchscope(scopes, " + current.getSQLScopeQuery() + ")";

                    if (current.hasNext()) {
                        if (current.isNextAnd()) {
                            sql += " and ";
                        } else {
                            sql += " or ";
                        }
                    }
                    current = current.getNext();
                }
                sql += ")";

            }

            sql += ") select csource.reg from a left join csource on a.cs_id = csource.id";
            if (subscription.getCsf() != null) {
                // if (sqlAdded) {
                // sql += " and ";
                // }
                // dollar++;
            }

            return client.preparedQuery(sql).execute(Tuple.from(tupleItems)).onFailure().retry().atMost(3);
        });
    }

}
