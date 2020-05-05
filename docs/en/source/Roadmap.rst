***************
Roadmap
***************
Scorpio broker Generic Enabler  is a java based pub-sub service designed and developed for the **FIWARE** platform, build on the top of **spring boot architecture** using **NGSI-LD** concepts,which allow you to collect,process,notify and store the Iot data with dynamic context with the use of linked data concepts .

Introduction
---------------

This section elaborates on proposed new features or tasks which are expected to be added to the product in the foreseeable future. There should be no assumption of a commitment to deliver these features on specific dates or in the order given. The development team will be doing their best to follow the proposed dates and priorities, but please bear in mind that plans to work on a given feature or task may be revised. All information is provided as general guidelines only, and this section may be revised to provide newer information at any time.

Disclaimer:

- This section has been last updated in April 2020. Please take into account its content could be obsolete.
- Note we develop this software in Agile way, so development plan is continuously under review. Thus, this road map has to be understood as rough plan of features to be done along time which is fully valid only at the time of writing it. This road map has not be understood as a commitment on features and/or dates.
- Some of the road map items may be implemented by external community developers, out of the scope of GE owners. Thus, the moment in which these features will be finalized cannot be assured.

Short Term
---------------

The following list of features are planned to be addressed in the short term, and incorporated in a next release of the product:

1.Issue tracking & general bug fixing: 

- Active tracking of the bugs with minimal response time and well defined structure to handle the issue tickets.

- The biggest chunk will be making the temporal component of Scorpio completely compliant to the NGSI-LD specification.

2.Road to full GE  

- Updating the documentation to fulfil the requirements of a full GE,with the step-by-step installation guide.

- Tutorial for each component and end points.
   
3.Provide multi-value support.  q params in subs and queries is already updated to support multivalues. Need to support for (updates and deletes based on dataset id)

 

Medium Term
-------------------

The following list of features are planned to be addressed in the medium term, typically within the subsequent release(s) generated in the next 6 months after the next planned release.

1.NGSI-LD Compliance Test-suit automation.

- Well tested and process compliant development cycle.

- Further test suit compliance.

2.MQTT Support 

- Better Error Reporting – in particular, a reason code has been added to responses for publications (PUBACK/PUBREC). MQTT originated with use cases like sensors along an oil pipeline – if their publications fail to be transmitted then the sensor will take no action. However, the use cases for MQTT are now much broader and an app on a phone may well want to warn the user if data is not being transmitted successfully. Return codes are now present on all acknowledgements (along with optional reason strings that contain human readable error diagnostics).

- Shared Subscriptions – If the message rate on a subscription is high, shared subscriptions can be used to load balance the messages across a number of receiving clients.

- Message Properties – Metadata in the header of a message. These are used to implement the other features in this list but also allow user defined properties e.g. to assist in message encryption by telling the receiver which key to use to decrypt the message contents.

- Message Expiry – An option to discard messages if they cannot be delivered within a user-defined period of time.

- Session Expiry – If a client does not connect within a user defined period of time, state (e.g. subscriptions and buffered messages) can be discarded without needing to be cleaned up.

- Topic Alias – Allows topic strings in messages to be replaced with a single number, reducing the number of bytes that need to be transmitted if a publisher repeatedly uses the same topics.

- Will Delay – Allows a message to be published if a client is disconnected for more than a user defined period of time. Allowing notifications about outages of important client applications without being swamped by false positives.

- Allowed Function Discovery – At the start of a connection, limits like the maximum packet size and number of (QoS>0) messages inflight can be transmitted to inform the client what it is allowed to do.

- Update notification for new endpoints (mqtt) (when spec is ready).Update parsing for NotificationParams in Subscription for custom client parameters 

3.Multi tenancy on database level, generic + auth 

- A multi-tenant application can provide savings by reducing development and deployment costs to companies that develop applications.

- These savings can be passed on to customers – increasing competitive advantages for all parties involved.

- Savings created by multi-tenancy come from sharing the same resources with multiple tenants.

- Sharing resources provides a way for an application vendor to create and maintain resources once for all customers, which can result in significant savings.

- Fragmented databases for multi tenant support are sometimes a legal requirement so we should support this. We should have a generic multi-tenant support switching databases based on a header and an extended version which expects this header to contain an auth token that we can validate and ensure access control.

4.Batch queries ( just a wrapper around multiple queries)
 
- Initial support for Batch queries.

- After potential update of the specification, update the return values.

5.NGSI-LD Test suite.
   
- Expanding the test suite with temporal queries.

6.query result count in return header
	
7.support offline mode with locally stored core context.

- Increase the performance by removing the delay of fetching the context data.

8.List entity types available (local only available) needs federation/registry support. 


Long term
-----------------

The following list of features are proposals regarding the longer-term evolution of the product even though the development of these features has not yet been scheduled for a release in the near future. Please feel free to contact us if you wish to get involved in the implementation or influence the road map:

1.Investigate access control on attribute level

- supporting multi tenants which should include addressing certain security aspects like access right management. 

2.@context cache requester

- providing a kind of a proxy for entity operations which stores @context.

3.Attribute Groups(have a new sub attributes for attributes )

4.Service Path on entity level new attrib needs to support hierarchy.

5.sql like functions for math (sum ,avg, min, max, count etc.) for a specific period  (e.g. give me average of every hour for the last day)
 
6.Experimental Web socket binding

- In order to prepare for future releases of the NGSI-LD spec we will develop an experimental (none standard) Websocket binding, supporting basic operations of NGSI-LD (query, create, update and subscribe)

7.Picture support micro-service (Scorpio exclusive, none standard) 

- The basic idea here is to have a micro-service which accepts pictures (with metadata) stores them in a file storage (s3, ftp, something...) and automatically generates an entity describing the picture and linking to the storage place.

- Additionally we might look into a (DT)service which analyses uploaded pictures and updates their entities automatically.