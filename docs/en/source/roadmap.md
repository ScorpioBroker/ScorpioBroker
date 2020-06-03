# Scorpio Broker Roadmap
Scorpio Broker: This product is an Incubated FIWARE Generic Enabler. If you would like to learn about the overall Roadmap of FIWARE, please check section "Roadmap" on the FIWARE Catalogue. The Scorpio Broker implements the NGSI-LD interface as specified by the ETSI Industry Specification Group on Context Information Management.

## Introduction
This section elaborates on proposed new features or tasks which are expected to be added to the product in the foreseeable future. There should be no assumption of a commitment to deliver these features on specific dates or in the order given. The development team will be doing their best to follow the proposed dates and priorities, but please bear in mind that plans to work on a given feature or task may be revised. All information is provided as a general guidelines only, and this section may be revised to provide newer information at any time.

Disclaimer:
 1. This section has been last updated in May 2020. Please take into account its content could be obsolete.
 2. Note we develop this software in an agile way, so the development plan is continuously under review. Thus, this roadmap has to be understood as rough plan of features to be done over time and it is fully valid only at the time of writing it. This roadmap has not be understood as a commitment on features and/or dates.
 3. Some of the roadmap items may be implemented by external community developers, out of the scope of GE owners. Thus, the moment in which these features will be finalized cannot be assured.

### Short Term

The following list of features are planned to be addressed in the short term, and incorporated in a next release of the product planned for September 2020:
1. Issue tracking & general bugfixing: We will continue to remove existing bugs and adress the current issues
   - Active tracking of the bugs with minimal response time and well defined structure to handle the issue tickets
   - The biggest chunk will be making the temporal component of Scorpio completely compliant to the NGSI-LD specification.
2. Road to full GE 
   - Updating the documentation to fulfil the requirements of a full GE, with the step-by-step installation guide.
   - Tutorial for component and end points.
   - Further requirements according to the list …
3. Full Multi-Attribute Support (almost complete)
4. Support offline mode with locally stored core context

### Medium Term
The following list of features are planned to be addressed in the medium term, typically within the subsequent release(s) generated in the next 6 months after the next planned release.
1. NGSI-LD Testsuite 
   - Expand the testsuite with temporal queries
   - Automation of testing
2. MQTT Support 
   - We have an initial support for mqtt endpoints on subscriptions. We will look into optimizing the approach
   - Adapt to the NGSI-LD MQTT Notification Binding newly agreed in ETSI ISG CIM
   - Looking further into MQTT 5 to potentially support a full NGSI-LD MQTT binding.
3. Initial multi-tenancy support on database level
4. Batch + POST Query operations 
   - Implement the POST query
5. Move entity count to specification compliant version to support result count on queries
6. List entity types available

### Long term
The following list of features are proposals regarding the longer-term evolution of the product even though the development of these features has not yet been scheduled for a release in the near future. Please feel free to contact us if you wish to get involved in the implementation or influence the roadmap:
1. Access control on entity and/or attribute level and complete multitenancy support
   - supporting multi tenants which should include addressing certain security aspects like access right management. This will need some discussion on the detail level of access rights etc.
2. @context cache requestor 
   - providing a kind of a proxy for entity operations which stores @context
3. Grouping of Attributes (successor of “Attribute Domains” from NGSIv1)
4. NGSI-LD Scope (extended FIWARE-Service-Path feature)
5. Support of aggregation operators in temporal query language
6. Experimental Websocket binding 
   - In order to prepare for future releases of the NGSI-LD spec we will develop an experimental (none standard) Websocket binding, supporting basic operations of NGSI-LD (query, create, update and subscribe)