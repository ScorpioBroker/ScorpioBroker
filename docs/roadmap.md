# Scorpio Broker Roadmap
Scorpio Broker: This product is an Incubated FIWARE Generic Enabler. If you would like to learn about the overall Roadmap of FIWARE, please check section "Roadmap" on the FIWARE Catalogue. The Scorpio Broker implements the NGSI-LD interface as specified by the ETSI Industry Specification Group on Context Information Management.

## Introduction
This section elaborates on proposed new features or tasks which are expected to be added to the product in the foreseeable future. There should be no assumption of a commitment to deliver these features on specific dates or in the order given. The development team will be doing their best to follow the proposed dates and priorities, but please bear in mind that plans to work on a given feature or task may be revised. All information is provided as a general guidelines only, and this section may be revised to provide newer information at any time.

Disclaimer:
 1. This section has been last updated in February 2021. Please take into account its content could be obsolete.
 2. Note we develop this software in an agile way, so the development plan is continuously under review. Thus, this roadmap has to be understood as rough plan of features to be done over time and it is fully valid only at the time of writing it. This roadmap has not be understood as a commitment on features and/or dates.
 3. Some of the roadmap items may be implemented by external community developers, out of the scope of GE owners. Thus, the moment in which these features will be finalized cannot be assured.

### Short Term

The following list of features are planned to be addressed in the short term, and incorporated in a next release of the product planned for end of the year or first half of 2021, in accordance with the next FIWARE release:
1. Issue tracking & general bugfixing: We will continue to remove existing bugs and adress the current issues
   - Active tracking of the bugs with minimal response time and well defined structure to handle the issue tickets
2. Coordinated move towards NGSI-LD v1.3.1 as this partially is breaking data structure and API calls. This merge will include
   - Support @context changes in NGSI-LD v1.3.1
   - Query Language Syntax Changes to Attribute Path (v1.3.1)
4. Implement the POST query
5. Work on Kubernetes support
   - Scaling support for individual micro services with Kubernetes 
   - Integration of micro services with Kubernetes services registry and gateway 

### Medium Term
The following list of features are planned to be addressed in the medium term, typically within the subsequent release(s) generated in the next 6 months after the next planned release.
1. Initial multi-tenancy support on database level
2. Support for queries where attribute is implicitly specified in query filter or geoquery
3. Websocket binding (initially for subscribe/notify interaction)

### Long term
The following list of features are proposals regarding the longer-term evolution of the product even though the development of these features has not yet been scheduled for a release in the near future. Please feel free to contact us if you wish to get involved in the implementation or influence the roadmap:
1. Access control on entity and/or attribute level and complete multitenancy support
   - supporting multi tenants which should include addressing certain security aspects like access right management. This will need some discussion on the detail level of access rights etc.
2. @context cache requestor 
   - providing a kind of a proxy for entity operations which stores @context
3. Grouping of Attributes (successor of “Attribute Domains” from NGSIv1)
4. NGSI-LD Scope (extended FIWARE-Service-Path feature)
5. Support of aggregation operators in temporal query language
6. Full MQTT Support 
   - Looking further into MQTT 5 to potentially support a full NGSI-LD MQTT binding.
