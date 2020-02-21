# Scorpio Roadmap
Scorpio is an incubated GE providing an NGSI-LD compliant interface.

## Introduction
This section elaborates on proposed new features or tasks which are expected to be added to the product in the foreseeable future. 
There should be no assumption of a commitment to deliver these features on specific dates or in the order given. 
The development team will be doing their best to follow the proposed dates and priorities, but please bear in mind that plans to work on a given feature or task may be revised. 
All information is provided as general guidelines only, and this section may be revised to provide newer information at any time.

Disclaimer:
 1. This section has been last updated in February 2020. Please take into account its content could be obsolete.
 2. Note we develop this software in an agile way, so the development plan is continuously under review. Thus, this roadmap has to be understood as rough plan of features to be done over time and it is fully valid only at the time of writing it. This roadmap has not be understood as a commitment on features and/or dates.
 3. Some of the roadmap items may be implemented by external community developers, out of the scope of GE owners. Thus, the moment in which these features will be finalized cannot be assured.

### Short Term

The following list of features are planned to be addressed in the short term, and incorporated in a next release of the product:
1. Issue tracking & general bugfixing: 
   We will continue to remove existing bugs and adress the current issues
   - The biggest chunk will be making the temporal component of scorpio completely compliant to the NGSI-LD specification.
2. Experimental Websocket binding
   - In order to prepare for future releases of the NGSI-LD spec we will develop an experimental (none standard) Websocket binding, supporting basic operations of NGSI-LD (query, create, update and subscribe)
3. MQTT Support 
   - We have an initial support for mqtt endpoints on subscriptions. We will look into optimizing the approach 
   - Looking further into MQTT 5 to potentially support an MQTT API endpoint.
4. NGSI-LD Testsuit
   - Further testsuit compliance 

### Medium Term
The following list of features are planned to be addressed in the medium term, typically within the subsequent release(s) generated in the next 6 months after the next planned release.
1. NGSI-LD Testsuite  
   - Expanding the testsuit with temporal queries
2. Provide multi-value support
3. Road to full GE
   - Updating the documentation to fullfil the requirements of a full GE (readthedocs)
   - Tutorial for the component 
4. Batch operations 
   - Initial support for Batch queries
   - After potential update of the specification, update the return values 
5. move entity count to specification compliant version (when defined) endpoint (additional header on queries)
   - support result count on queries
6. support offline mode with locally stored core context 

### Long term
The following list of features are proposals regarding the longer-term evolution of the product even though the development of these features has not yet been scheduled for a release in the near future. Please feel free to contact us if you wish to get involved in the implementation or influence the roadmap:
1. multi tenant + security 
   - supporting multi tenants which should include adressing certain security aspects like access right management. This will need some discussion on the detail level of access rights etc.
2. @context cache requestor
   - providing a kind of a proxy for entity operations which stores @context
