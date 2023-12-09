*************************************
Explicit @context in Subscriptions
*************************************

In previous versions, the @context used for notifications was implicitly set to the @context used for the subscription, without allowing the user to change it. In NGSI-LD v1.7.1, we have made this @context explicit. User can now add a field called *jsonldContext* to specify the explicit @context in the subscription.

**jsonldContext** – The dereferenceable URI of the JSON-LD @context to be used when sending a notification resulting from the subscription. If not provided, the @context used for the subscription shall be used as a default.

Example for Explicit @context in Subscriptions
-------------------------------------------------

To create a subscription with an explicit @context, you can send a POST request to the following endpoint: **http://<IP Address>:<port>/ngsi-ld/v1/subscriptions/** with the provided payload.

.. code-block:: JSON

	{
		"id": "urn:ngsi-ld:Subscription:testSubscription1",
		"type": "Subscription",
		"entities": [
			{
				"type": "TestType"
			}
		],
		"notification": {
			"format": "normalized",
			"endpoint": {
				"uri": "http://localhost:8080",
				"accept": "application/json"
			}
		},
		"jsonldContext": "http://localhost:9090/ngsi-ld/v1/jsonldContexts/urn:e732d9eb-2e0e-491c-8b26-c6a39136f2ff"
	}