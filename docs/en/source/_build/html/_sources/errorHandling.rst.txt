***********************
Error Handler
***********************

This section will provide info on the error handling mechanism for the Scorpio Broker system.

Listed below are the events of the system

.. list-table::  **Error Handling** 
   :widths: 5 15 35 15 10 20
   :header-rows: 1

   * - S.No.				
     - Operation/Event 	 		 
     - Scenario Description
     - Responsible  Module
     - Error Code/ Response
     - Action
      
   * - 1.
     - InvalidRequest								
     - The request associated to the operation is syntactically invalid or includes wrong content	
     - REST Controller
     - HTTP 400
     - Log the error & notify the requestor

   * - 2.
     - BadRequestData								
     - The request includes input data which does not meet the requirements of the operation	
     - REST Controller
     - HTTP 400
     - Log the error & notify the requestor

   * - 3.
     - AlreadyExists								
     - The referred element already exists	
     - REST Controller
     - HTTP 409
     - Log the error & notify the requestor

   * - 4.
     - OperationNotSupported								
     - The operation is not supported	
     - REST Controller
     - HTTP 422
     - Log the error & notify the requestor

   * - 5.
     - ResourceNotFound								
     - The referred resource has not been found	
     - REST Controller
     - HTTP 404
     - Log the error & notify the requestor

   * - 6.
     - InternalError								
     - There has been an error during the operation execution	
     - REST Controller
     - HTTP 500
     - Log the error & notify the requestor

   * - 7.
     - Method Not Allowed								
     - There has been an error when a client invokes a wrong HTTP verb over a resource	
     - REST Controller
     - HTTP 405
     - Log the error & notify the requestor




Please note the errors can also be categorized into following categories for different exceptions that can occur internally to the implementation logic as well:

 1. Low criticality is those which involve the errors that should be handled by the software logic, and are due to some configuration issues and should not require anything like reset, a reboot of the system.

 2. Medium Criticality is those which will be tried for the software logic handling but it may need system reset, chip reset and may interrupt system significantly.

 3. High Criticality is the hardware-based error that should not occur and if occur may need system reset.

Fail-safe mechanisms for the different category of errors:

 a. For the Low criticality of the errors, logging will be performed, the retry will be performed and error will be handled by means of rollback and sending failure to the upper layers.

 b.For the High Criticality errors, emergency errors will be logged further recommending a reboot.

 c.For the Medium criticality errors logging, retry mechanisms will be implemented further logging emergency logs to the system and recommend a reboot to the administrator. 

During the initialization, failure will be logged as emergency and error will be returned to the calling program