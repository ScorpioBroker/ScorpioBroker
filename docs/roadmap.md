# Scorpio Broker Roadmap
Scorpio Broker: This product is an Incubated FIWARE Generic Enabler. If you would like to learn about the overall Roadmap of FIWARE, please check section "Roadmap" on the FIWARE Catalogue. The Scorpio Broker implements the NGSI-LD interface as specified by the ETSI Industry Specification Group on Context Information Management.

## Introduction
This section elaborates on proposed new features or tasks which are expected to be added to the product in the foreseeable future. There should be no assumption of a commitment to deliver these features on specific dates or in the order given. The development team will be doing their best to follow the proposed dates and priorities, but please bear in mind that plans to work on a given feature or task may be revised. All information is provided as a general guidelines only, and this section may be revised to provide newer information at any time.

Disclaimer:
 1. This section has been last updated in March 2022. Please take into account its content could be obsolete.
 2. Note we develop this software in an agile way, so the development plan is continuously under review. Thus, this roadmap has to be understood as rough plan of features to be done over time and it is fully valid only at the time of writing it. This roadmap has not be understood as a commitment on features and/or dates.
 3. Some of the roadmap items may be implemented by external community developers, out of the scope of GE owners. Thus, the moment in which these features will be finalized cannot be assured.

### Short Term

The following list of features are planned to be addressed in the short term, and incorporated in a next release of the product planned for the end of first half of 2022, in accordance with the next FIWARE release:
1. Issue tracking & general bugfixing: We will continue to remove existing bugs and address the current issues 🕑
   - Active tracking of the bugs with minimal response time and well defined structure to handle the issue tickets
2. Feature changes & Testing
   - Adapt the new ETSI test suite as default system test
   - DB migration scripts to get 1.2.1 entities transformed to 1.3.1
3. IoT Agent Integration
4. Multi-typing  (1.5)


### Medium Term
The following list of features are planned to be addressed in the medium term, typically within the subsequent release(s) generated in the next 6 months after the next planned release.
1. Batch operations with multiple instances of same entity (1.5)
2. Multi-language properties (1.4)
3. Storing, Managing & Serving @context (1.5)
4. Move to Quarkus microservice framework
5. Move to native images using GraalVM

### Long term
The following list of features are proposals regarding the longer-term evolution of the product even though the development of these features has not yet been scheduled for a release in the near future. Please feel free to contact us if you wish to get involved in the implementation or influence the roadmap:
1. Access control on entity and/or attribute level and complete multitenancy support
   - supporting multi tenants which should include addressing certain security aspects like access right management. This will need some discussion on the detail level of access rights etc.
2. Storing, Managing & Serving @context (NGSI-LD v1.5.1) 
3. Enhancements for temporal queries
   - Support of aggregation operators in temporal query language (NGSI-LD v1.4.1) 
   - Pagination for temporal attributes (NGSI-LD v1.5.1)
4. Multi-language properties (NGSI-LD v1.4.1) 
5. Full MQTT Support 
   - Looking further into MQTT 5 to potentially support a full NGSI-LD MQTT binding.
6. Grouping of Attributes (successor of “Attribute Domains” from NGSIv1)
7. Websocket binding (initially for subscribe/notify interaction) 
