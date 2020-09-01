*******************************
API Walkthrough
*******************************

.. list-table::  **API List** 
   :widths: 5 30 15 35 15
   :header-rows: 1

   * - S.No.				
     - External Interface 	 		 
     - Module name
     - Description
     - Http verb
     

   * - 1.
     - http://ip:port/ngsi-ld/v1/entities								
     - Entity	
     - To create entity.
     - POST

   * - 2.
     - http://ip:port/ngsi-ld/v1/entities/{entityId}/attrs									
     - Entity
     - To add attributes to an entity.
     - POST

   * - 3.
     - http://ip:port/ngsi-ld/v1/entities								
     - Entity	.
     - To retrieve the list of existing entity
     - GET

   * - 4.
     - http://ip:port/ngsi-ld/v1/entities/{entityId}							
     - Entity
     - To retrieve an entity by its ID	
     - GET

   * - 5.
     - http://ip:port/ngsi-ld/v1/entities/{entityId}/attrs									
     - Entity
     - To update the attributes of an entity.
     - PATCH

   * - 6.
     - http://ip:port/ngsi-ld/v1/entities/{entityId}/attrs /{attrId}									
     - Entity
     - To partially update an attribute
     - PATCH

   * - 7.
     - http://ip:port/ngsi-ld/v1/entities/{entityId}									
     - Entity
     - To delete an entity by its ID
     - DELETE

   * - 8.
     - http://ip:port/ngsi-ld/v1/entities/{entityId}/attrs /{attrId}									
     - Entity
     - To delete an attribute.
     - DELETE


   * - 9.
     - http://ip:port/ngsi-ld/v1/subscriptions						
     - Subscription
     - To add subscription in the Scorpio Broker
     - POST

   * - 10.
     - http://ip:port/ngsi-ld/v1/subscriptions							
     - Subscription
     - To retrieve subscription list	
     - GET

   * - 11.
     - http://ip:port/ngsi-ld/v1/subscriptions/{subscriptionID}							
     - Subscription
     - To retrieve subscription details of a particular subscription
     - GET

   * - 12.
     - http://ip:port/ngsi-ld/v1/subscriptions/{subscriptionID}									
     - Subscription
     - To update the already existing subscription.
     - PATCH

   * - 13.
     - http://ip:port/ngsi-ld/v1/subscriptions/{subscriptionID}								
     - Subscription
     - To delete a subscription
     - DELETE	

   * - 14.
     - http://ip:port/ngsi-ld/v1/csources									
     - Context Source Registration
     - To add register a csource in the Scorpio Broker
     - POST

   * - 15.
     - http://ip:port/ngsi-ld/v1/csources								
     - Context Source Registration
     - To retrieve list of Context Source which are  registered
     - GET

   * - 16.
     - http://ip:port/ngsi-ld/v1/csources/{registrationId}									
     - Context Source Registration
     - To retrieve subscription details of a particular csource registration
     - GET

   * - 17.
     - http://ip:port/ngsi-ld/v1/csources/{registrationId}									
     - Context Source Registration
     - To update the already existing csource registration.
     - PATCH

   * - 18.
     - http://ip:port/ngsi-ld/v1/csources/{registrationId}						
     - Context Source Registration
     - To delete a resgistration.	
     - DELETE

   * - 19.
     - http://ip:port/ngsi-ld/v1/csourceSubscriptions									
     - Context Source Registration Subscription
     - To add subscription for csource in the Scorpio Broker(device)
     - POST

   * - 20.
     - http://ip:port/ngsi-ld/v1/csourceSubscriptions									
     - Context Source Registration Subscription
     - To retrieve the subscription list of context registration.
     - GET

   * - 21.
     - http://ip:port/ngsi-ld/v1/csourceSubscriptions/{subscriptionId}								
     - Context Source Registration Subscription
     - To retrieve subscription details of  csource registration by id
     - GET

   * - 22.
     - http://ip:port/ngsi-ld/v1/csourceSubscriptions/{subscriptionId}									
     - Context Source Registration Subscription
     - To update the subscription of csource registration.
     - PATCH

   * - 23.
     - http://ip:port/ngsi-ld/v1/csourceSubscriptions/{subscriptionId}									
     - Context Source Registration Subscription
     - To delete the subscription of csource registration.
     - DELETE
