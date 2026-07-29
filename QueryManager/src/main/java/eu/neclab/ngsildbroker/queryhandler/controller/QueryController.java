package eu.neclab.ngsildbroker.queryhandler.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.metrics.MetricUnits;
import org.eclipse.microprofile.metrics.annotation.ConcurrentGauge;
import org.eclipse.microprofile.metrics.annotation.Counted;
import org.eclipse.microprofile.metrics.annotation.Timed;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.net.HttpHeaders;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.ParsedQueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.ViaHeaders;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.DataSetIdTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.OmitTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.OrderByTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.PickTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.Query;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.NgsiLdOperation;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.commons.tools.QueryParamParser;
import eu.neclab.ngsildbroker.commons.tools.QueryParser;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;
import io.quarkus.runtime.Startup;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple3;
import io.smallrye.mutiny.tuples.Tuple5;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;

@ApplicationScoped
@Startup
@Path("/ngsi-ld/v1")
public class QueryController {

	private static Logger logger = LoggerFactory.getLogger(QueryController.class);

	@Inject
	JsonLDService ldService;

	@Inject
	QueryService queryService;

	@Inject
	MicroServiceUtils microServiceUtils;

	@ConfigProperty(name = "scorpio.entity.default-limit")
	int defaultLimit;

	@ConfigProperty(name = "scorpio.entity.max-limit")
	int maxLimit;

	@ConfigProperty(name = "scorpio.ngsild.corecontext")
	String coreContext;

	/**
	 * Method(GET) for multiple attributes separated by comma list
	 *
	 * @param request
	 * @param entityId
	 * @param attrs
	 * @return
	 * @throws ResponseException
	 */
	@Path("/entities/{entityId}")
	@GET
	@Counted(name = "entity_retrieve_total", description = "Total number of entity retrieve requests", absolute = true)
	@Timed(name = "entity_retrieve_duration", description = "Duration of entity retrieve requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "entity_retrieve_concurrent", description = "Number of concurrent entity retrieve requests", absolute = true)
	public Uni<RestResponse<Object>> getEntity(HttpServerRequest request, @PathParam("entityId") String entityId,
			@HeaderParam("NGSILD-EntityMap") String entityMapToken) {
		logger.debug("getEntity");
		String tenant = HttpUtils.getTenant(request);
		String attrs;
		String options;
		String lang;
		String geometryProperty;
		String containedBy;
		String join;
		Integer joinLevel;
		String pick;
		String omit;
		String format;
		String datasetId;
		String typeQuery;
		boolean localOnly;
		boolean doNotCompact;
		boolean entityMap;
		boolean distEntities;
		try {
			ParsedQueryParams queryParams = QueryParamParser.parse(request, NgsiLdOperation.RETRIEVE_ENTITY);
			attrs = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ATTRS);
			options = queryParams.getString(NGSIConstants.QUERY_PARAMETER_OPTIONS);
			lang = queryParams.getString(NGSIConstants.QUERY_PARAMETER_LANG);
			geometryProperty = queryParams.getString(NGSIConstants.QUERY_PARAMETER_GEOMETRY_PROPERTY);
			containedBy = queryParams.getString(NGSIConstants.QUERY_PARAMETER_CONTAINED_BY, AppConstants.EMPTY);
			join = queryParams.getString(NGSIConstants.QUERY_PARAMETER_JOIN);
			joinLevel = queryParams.getInteger(NGSIConstants.QUERY_PARAMETER_JOINLEVEL);
			pick = queryParams.getString(NGSIConstants.QUERY_PARAMETER_PICK);
			omit = queryParams.getString(NGSIConstants.QUERY_PARAMETER_OMIT);
			format = queryParams.getString(NGSIConstants.QUERY_PARAMETER_FORMAT);
			datasetId = queryParams.getString(NGSIConstants.QUERY_PARAMETER_DATA_SET_ID);
			typeQuery = queryParams.getString(NGSIConstants.QUERY_PARAMETER_TYPE);
			localOnly = queryParams.getLocal();
			doNotCompact = queryParams.getBoolean(NGSIConstants.QUERY_PARAMETER_DO_NOT_COMPACT);
			entityMap = queryParams.getBoolean(NGSIConstants.QUERY_PARAMETER_ENTITY_MAP);
			distEntities = queryParams.getBoolean(NGSIConstants.QUERY_PARAMETER_SPLIT_ENTITIES, true);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		return queryForQueryResult(request, entityId, typeQuery, null, attrs, null, null, null, null, null, null,
				geometryProperty, lang, null, localOnly, options, 1, 0, false, containedBy, join, joinLevel,
				doNotCompact, entityMapToken, entityMap, pick, omit, format, null, datasetId, distEntities,
				null, null, null, null, false)
				.onItemOrFailure().transformToUni((t, e) -> {
					if (e != null) {
						return Uni.createFrom().failure(e);
					}
					QueryResult queryResult = t.getItem1();
					if (queryResult.getData().isEmpty()) {
						return Uni.createFrom()
								.failure(new ResponseException(ErrorType.NotFound, entityId + " was not found"));
					}
					return HttpUtils.generateQueryResult(request, queryResult, t.getItem2(), geometryProperty,
							t.getItem3(), false, t.getItem4(), queryResult.getLanguageQueryTerm(), t.getItem5(),
							ldService, false, false, entityMap, microServiceUtils.getGatewayString(),
							NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT, AppConstants.ENTITY_RETRIEVED_PAYLOAD);
				}).onFailure()
				.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, tenant));

	}

	@Path("/entities")
	@GET
	@Counted(name = "entity_queries_total", description = "Total number of entity query requests", absolute = true)
	@Timed(name = "entity_query_duration", description = "Duration of entity query requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "entity_queries_concurrent", description = "Number of concurrent entity query requests", absolute = true)
	public Uni<RestResponse<Object>> query(HttpServerRequest request,
			@HeaderParam(NGSIConstants.HEADER_ENTITY_MAP) String entityMapToken) {
		logger.debug("query");
		String tenant = HttpUtils.getTenant(request);
		String id;
		String typeQuery;
		String idPattern;
		String attrs;
		String q;
		String csf;
		String geometry;
		String georel;
		String coordinates;
		String geoproperty;
		String geometryProperty;
		String lang;
		String scopeQ;
		String options;
		Integer limit;
		int offset;
		String containedBy;
		String join;
		Integer joinLevel;
		String pick;
		String omit;
		String format;
		String jsonKeysQP;
		String datasetId;
		String orderBy;
		String orderFrom;
		String orderGeometry;
		String collation;
		boolean localOnly;
		boolean doNotCompact;
		boolean entityMapRetrieve;
		boolean distEntities;
		boolean count;
		boolean metadata;
		try {
			ParsedQueryParams queryParams = QueryParamParser.parse(request, NgsiLdOperation.QUERY_ENTITIES);
			id = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ID);
			typeQuery = queryParams.getString(NGSIConstants.QUERY_PARAMETER_TYPE);
			idPattern = queryParams.getString(NGSIConstants.QUERY_PARAMETER_IDPATTERN);
			attrs = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ATTRS);
			q = queryParams.getQ();
			csf = queryParams.getString(NGSIConstants.QUERY_PARAMETER_CSF);
			geometry = queryParams.getString(NGSIConstants.QUERY_PARAMETER_GEOMETRY);
			georel = queryParams.getGeorel();
			coordinates = queryParams.getString(NGSIConstants.QUERY_PARAMETER_COORDINATES);
			geoproperty = queryParams.getString(NGSIConstants.QUERY_PARAMETER_GEOPROPERTY);
			geometryProperty = queryParams.getString(NGSIConstants.QUERY_PARAMETER_GEOMETRY_PROPERTY);
			lang = queryParams.getString(NGSIConstants.QUERY_PARAMETER_LANG);
			scopeQ = queryParams.getScopeQ();
			options = queryParams.getString(NGSIConstants.QUERY_PARAMETER_OPTIONS);
			limit = queryParams.getInteger(NGSIConstants.QUERY_PARAMETER_LIMIT);
			offset = queryParams.getInt(NGSIConstants.QUERY_PARAMETER_OFFSET, 0);
			containedBy = queryParams.getString(NGSIConstants.QUERY_PARAMETER_CONTAINED_BY, AppConstants.EMPTY);
			join = queryParams.getString(NGSIConstants.QUERY_PARAMETER_JOIN);
			joinLevel = queryParams.getInteger(NGSIConstants.QUERY_PARAMETER_JOINLEVEL);
			pick = queryParams.getString(NGSIConstants.QUERY_PARAMETER_PICK);
			omit = queryParams.getString(NGSIConstants.QUERY_PARAMETER_OMIT);
			format = queryParams.getString(NGSIConstants.QUERY_PARAMETER_FORMAT);
			jsonKeysQP = queryParams.getString(NGSIConstants.QUERY_PARAMETER_JSON_KEYS);
			datasetId = queryParams.getString(NGSIConstants.QUERY_PARAMETER_DATA_SET_ID);
			orderBy = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ORDER_BY);
			orderFrom = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ORDER_FROM);
			orderGeometry = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ORDER_GEOMETRY);
			collation = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ORDER_COLLATION);
			localOnly = queryParams.getLocal();
			doNotCompact = queryParams.getBoolean(NGSIConstants.QUERY_PARAMETER_DO_NOT_COMPACT);
			entityMapRetrieve = queryParams.getBoolean(NGSIConstants.QUERY_PARAMETER_ENTITY_MAP);
			distEntities = queryParams.getBoolean(NGSIConstants.QUERY_PARAMETER_SPLIT_ENTITIES, true);
			count = queryParams.getBoolean(NGSIConstants.QUERY_PARAMETER_COUNT);
			metadata = queryParams.getBoolean(NGSIConstants.QUERY_PARAMETER_META_DATA, false);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		return queryForQueryResult(request, id, typeQuery, idPattern, attrs, q, csf, geometry, georel,
				coordinates, geoproperty, geometryProperty, lang, scopeQ, localOnly, options, limit, offset, count,
				containedBy, join, joinLevel, doNotCompact, entityMapToken, entityMapRetrieve,
				pick, omit, format, jsonKeysQP, datasetId, distEntities, orderBy, orderFrom, orderGeometry, collation,
				metadata)
				.onItemOrFailure().transformToUni((t, e) -> {
					if (e != null) {
						return Uni.createFrom().failure(e);
					}
					QueryResult queryResult = t.getItem1();
					Set<String> finalOptions = t.getItem2();
					Integer acceptHeader = t.getItem3();
					Integer actualLimit = t.getItem4();
					Context context = t.getItem5();
					if (doNotCompact) {
						return Uni.createFrom().item(RestResponse.ok((Object) queryResult.getData()));
					}

					return HttpUtils.generateQueryResult(request, queryResult, finalOptions, geometryProperty,
							acceptHeader, count, actualLimit, queryResult.getLanguageQueryTerm(), context, ldService,
							entityMapRetrieve, microServiceUtils.getGatewayString(),
							NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT, AppConstants.QUERY_PAYLOAD);
				}).onFailure()
				.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, tenant));
	}

	@Path("/types")
	@GET
	@Counted(name = "types_total", description = "Total number of types requests", absolute = true)
	@Timed(name = "types_duration", description = "Duration of types requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "types_concurrent", description = "Number of concurrent types requests", absolute = true)
	public Uni<RestResponse<Object>> getAllTypes(HttpServerRequest request) {
		logger.debug("getAllTypes");
		boolean details;
		boolean localOnly;
		boolean bbox;
		String tenant = HttpUtils.getTenant(request);
		try {
			ParsedQueryParams queryParams = QueryParamParser.parse(request, NgsiLdOperation.RETRIEVE_TYPES);
			details = queryParams.getBoolean(NGSIConstants.QUERY_PARAMETER_DETAILS);
			localOnly = queryParams.getLocal();
			bbox = queryParams.getBoolean(NGSIConstants.QUERY_PARAMETER_BBOX, false);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		HttpUtils.getAtContext(request);
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll(HttpHeaders.ACCEPT));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		List<Object> contextHeader = HttpUtils.getAtContext(request);
		return HttpUtils.getContext(contextHeader, ldService).onItem().transformToUni(context -> {

			return queryService
					.getTypes(tenant, localOnly, request.headers(), details, bbox, viaHeaders)
					.onItem()
					.transformToUni(types -> {
						return HttpUtils.generateResult(contextHeader, context, acceptHeader, types, null,
								null, null, ldService, null, null, false, true, -1);
					});

		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, tenant));

	}

	@Path("/types/{entityType}")
	@GET
	@Counted(name = "types_type_total", description = "Total number of type requests", absolute = true)
	@Timed(name = "types_type_duration", description = "Duration of type requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "types_type_concurrent", description = "Number of concurrent type requests", absolute = true)
	public Uni<RestResponse<Object>> getType(HttpServerRequest request, @PathParam("entityType") String type) {
		logger.debug("getType");
		boolean localOnly;
		String tenant = HttpUtils.getTenant(request);
		try {
			ParsedQueryParams queryParams = QueryParamParser.parse(request, NgsiLdOperation.RETRIEVE_TYPE);
			localOnly = queryParams.getLocal();
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll(HttpHeaders.ACCEPT));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		List<Object> contextHeader = HttpUtils.getAtContext(request);
		return HttpUtils.getContext(contextHeader, ldService).onItem().transformToUni(context -> {
			return queryService.getType(tenant, context.expandIri(type, false, true, null, null),
					localOnly, request.headers(), false).onItem().transformToUni(map -> {
						if (map.isEmpty()) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
						} else {
							return HttpUtils.generateResult(contextHeader, context, acceptHeader, map, null,
									null, null, ldService, null, null, false, true, -1);
						}
					});
		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, tenant));

	}

	@Path("/attributes")
	@GET
	@Counted(name = "attributes_total", description = "Total number of attributes requests", absolute = true)
	@Timed(name = "attributes_duration", description = "Duration of attributes requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "attributes_concurrent", description = "Number of concurrent attributes requests", absolute = true)
	public Uni<RestResponse<Object>> getAllAttributes(HttpServerRequest request) {
		logger.debug("getAllAttributes");
		boolean localOnly;
		boolean details;
		String tenant = HttpUtils.getTenant(request);
		try {
			ParsedQueryParams queryParams = QueryParamParser.parse(request, NgsiLdOperation.RETRIEVE_ATTRIBUTES);
			localOnly = queryParams.getLocal();
			details = queryParams.getBoolean(NGSIConstants.QUERY_PARAMETER_DETAILS);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll(HttpHeaders.ACCEPT));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		List<Object> contextHeader = HttpUtils.getAtContext(request);
		return HttpUtils.getContext(contextHeader, ldService).onItem().transformToUni(context -> {
			if (!details) {
				return queryService.getAttribs(tenant, localOnly, request.headers()).onItem()
						.transformToUni(map -> {
							return HttpUtils.generateResult(contextHeader, context, acceptHeader, map, null,
									null, null, ldService, null, null, false, true, -1);
						});
			} else {
				return queryService.getAttribsWithDetails(tenant, localOnly, request.headers())
						.onItem().transformToUni(list -> {
							return HttpUtils.generateResult(contextHeader, context, acceptHeader, list, null,
									null, null, ldService, null, null, false, true, -1);
						});
			}
		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, tenant));

	}

	@Path("/attributes/{attribute}")
	@GET
	@Counted(name = "attributes_attribute_total", description = "Total number of attribute requests", absolute = true)
	@Timed(name = "attributes_attribute_duration", description = "Duration of attribute requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "attributes_attribute_concurrent", description = "Number of concurrent attribute requests", absolute = true)
	public Uni<RestResponse<Object>> getAttribute(HttpServerRequest request, @PathParam("attribute") String attribute) {
		logger.debug("getAttribute");
		boolean localOnly;
		boolean details;
		String tenant = HttpUtils.getTenant(request);
		try {
			ParsedQueryParams queryParams = QueryParamParser.parse(request, NgsiLdOperation.RETRIEVE_ATTRIBUTE);
			localOnly = queryParams.getLocal();
			details = queryParams.getBoolean(NGSIConstants.QUERY_PARAMETER_DETAILS);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll(HttpHeaders.ACCEPT));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}

		List<Object> contextHeader = HttpUtils.getAtContext(request);
		return HttpUtils.getContext(contextHeader, ldService).onItem().transformToUni(context -> {
			return queryService.getAttrib(tenant,
					context.expandIri(attribute, false, true, null, null), localOnly, request.headers()).onItem()
					.transformToUni(map -> {
						if (map.isEmpty()) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
						} else {
							return HttpUtils.generateResult(contextHeader, context, acceptHeader, map, null,
									null, null, ldService, null, null, false, true, -1);
						}
					});
		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, tenant));

	}

	@Path("/entityMap")
	@GET
	@Counted(name = "entity_map_create_total", description = "Total number of entitymap create requests", absolute = true)
	@Timed(name = "entity_map_create_duration", description = "Duration of entitymap create requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "entity_map_create_concurrent", description = "Number of concurrent entitymap create requests", absolute = true)
	public Uni<RestResponse<Object>> queryEntityMap(HttpServerRequest request) {
		logger.debug("queryEntityMap");
		String tenant = HttpUtils.getTenant(request);
		String id;
		String typeQuery;
		String idPattern;
		String attrs;
		String q;
		String csf;
		String geometry;
		String georel;
		String coordinates;
		String geoproperty;
		String geometryProperty;
		String lang;
		String scopeQ;
		String pick;
		String omit;
		String jsonKeysQP;
		String datasetId;
		String orderBy;
		String orderFrom;
		String orderGeometry;
		String collation;
		boolean distEntities;
		try {
			ParsedQueryParams queryParams = QueryParamParser.parse(request, NgsiLdOperation.CREATE_ENTITY_MAP);
			id = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ID);
			typeQuery = queryParams.getString(NGSIConstants.QUERY_PARAMETER_TYPE);
			idPattern = queryParams.getString(NGSIConstants.QUERY_PARAMETER_IDPATTERN);
			attrs = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ATTRS);
			q = queryParams.getQ();
			csf = queryParams.getString(NGSIConstants.QUERY_PARAMETER_CSF);
			geometry = queryParams.getString(NGSIConstants.QUERY_PARAMETER_GEOMETRY);
			georel = queryParams.getGeorel();
			coordinates = queryParams.getString(NGSIConstants.QUERY_PARAMETER_COORDINATES);
			geoproperty = queryParams.getString(NGSIConstants.QUERY_PARAMETER_GEOPROPERTY);
			geometryProperty = queryParams.getString(NGSIConstants.QUERY_PARAMETER_GEOMETRY_PROPERTY);
			lang = queryParams.getString(NGSIConstants.QUERY_PARAMETER_LANG);
			scopeQ = queryParams.getScopeQ();
			pick = queryParams.getString(NGSIConstants.QUERY_PARAMETER_PICK);
			omit = queryParams.getString(NGSIConstants.QUERY_PARAMETER_OMIT);
			jsonKeysQP = queryParams.getString(NGSIConstants.QUERY_PARAMETER_JSON_KEYS);
			datasetId = queryParams.getString(NGSIConstants.QUERY_PARAMETER_DATA_SET_ID);
			orderBy = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ORDER_BY);
			orderFrom = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ORDER_FROM);
			orderGeometry = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ORDER_GEOMETRY);
			collation = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ORDER_COLLATION);
			distEntities = queryParams.getBoolean(NGSIConstants.QUERY_PARAMETER_SPLIT_ENTITIES, true);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		return getQueryParam(request, id, typeQuery, idPattern, attrs, q, csf, geometry, georel, coordinates,
				geoproperty, geometryProperty, lang, scopeQ, false, null, 1, 0, false, null, null, -1, false, null,
				false, pick, omit, null, jsonKeysQP, datasetId, distEntities, orderBy,
				orderFrom,
				orderGeometry, collation, false).onItem()
				.transformToUni(params -> {
					return queryService.getAndStoreEntityMap(tenant, params.getEntityMapToken(),
							params.getIdsAndTypeAndIdPattern(), params.getAttrsQueryTerm(), params.getGeoQueryTerm(),
							params.getqQueryTerm(), params.getCsfQueryTerm(), params.getScopeQueryTerm(),
							params.getLanguageQueryTerm(), 1, 0,
							params.getContext(), request.headers(), false, params.getDataSetIdTerm(), null, -1,
							distEntities, params.getPickTerm(), params.getOmitTerm(), params.getCheckSum(),
							params.getViaHeaders(), null, false, true, true, true, params.getOrderBy(),
							params.isMetadata()).onItem()
							.transform(t -> {
								return HttpUtils.generateEntityMapResult(t.getItem2());
							});
				}).onFailure().recoverWithItem(
						e -> HttpUtils.handleControllerExceptions(e, tenant));

	}

	@Path("/entityMap/{entityMapId}")
	@GET
	@Counted(name = "entity_map_retrieve_total", description = "Total number of entitymap retrieve requests", absolute = true)
	@Timed(name = "entity_map_retrieve_duration", description = "Duration of entitymap retrieve requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "entity_map_retrieve_concurrent", description = "Number of concurrent entitymap retrieve requests", absolute = true)
	public Uni<RestResponse<Object>> getEntityMap(HttpServerRequest request,
			@PathParam("entityMapId") String entityMapId) {
		logger.debug("getEntityMap");
		String tenant = HttpUtils.getTenant(request);
		try {
			QueryParamParser.parse(request, NgsiLdOperation.RETRIEVE_ENTITY_MAP);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		return queryService.getEntityMap(tenant, entityMapId).onItem()
				.transform(entityMap -> HttpUtils.generateEntityMapResult(entityMap)).onFailure()
				.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, tenant));

	}

	@Path("/entityMap/{entityMapId}")
	@DELETE
	@Counted(name = "entity_map_delete_total", description = "Total number of entitymap delete requests", absolute = true)
	@Timed(name = "entity_map_delete_duration", description = "Duration of entitymap delete requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "entity_map_delete_concurrent", description = "Number of concurrent entitymap delete requests", absolute = true)
	public Uni<RestResponse<Object>> deleteEntityMap(HttpServerRequest request,
			@PathParam("entityMapId") String entityMapId) {
		logger.debug("deleteEntityMap");
		String tenant = HttpUtils.getTenant(request);
		try {
			QueryParamParser.parse(request, NgsiLdOperation.DELETE_ENTITY_MAP);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		return queryService.deleteEntityMap(tenant, entityMapId).onItem()
				.transform(v -> RestResponse.status(204)).onFailure()
				.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, tenant));

	}

	@Path("/entityMap/{entityMapId}")
	@PATCH
	@Counted(name = "entity_map_patch_total", description = "Total number of entitymap patch requests", absolute = true)
	@Timed(name = "entity_map_patch_duration", description = "Duration of entitymap patch requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "entity_map_patch_concurrent", description = "Number of concurrent entitymap patch requests", absolute = true)
	public Uni<RestResponse<Object>> updateEntityMap(HttpServerRequest request, String bodyStr,
			@PathParam("entityMapId") String entityMapId) {
		logger.debug("updateEntityMap");
		try {
			QueryParamParser.parse(request, NgsiLdOperation.UPDATE_ENTITY_MAP);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		return JsonUtils.fromString(bodyStr).onItem().transformToUni(obj -> {
			@SuppressWarnings("unchecked")
			Map<String, Object> body = (Map<String, Object>) obj;
			String expiresAt = (String) body.get(NGSIConstants.EXPIRES_AT);
			String tenant = HttpUtils.getTenant(request);
			return queryService.updateEntityMap(tenant, entityMapId, expiresAt).onItem()
					.transform(v -> RestResponse.status(204)).onFailure()
					.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, tenant));

		});

	}

	public Uni<Tuple5<QueryResult, Set<String>, Integer, Integer, Context>> queryForQueryResult(
			HttpServerRequest request,
			String id, String typeQuery, String idPattern, String attrs, String q, String csf, String geometry,
			String georel, String coordinates, String geoproperty, String geometryProperty, String lang,
			String scopeQ, boolean localOnly, String options, Integer limit, int offset, boolean count,
			String containedBy, String join, Integer joinLevelInput, boolean doNotCompact, String entityMapToken,
			boolean entityMapRetrieve, String pick, String omit, String format,
			String jsonKeysQP, String datasetId, boolean distEntities, String orderBy, String orderFrom,
			String orderGeometry, String collation, boolean metadata) {
		int joinLevel;
		if (joinLevelInput == null) {
			if (join == null) {
				joinLevel = 0;
			} else {
				joinLevel = 1;
			}
		} else {
			joinLevel = joinLevelInput;
		}
		String tenant = HttpUtils.getTenant(request);
		return getQueryParam(request, id, typeQuery, idPattern, attrs, q, csf, geometry, georel, coordinates,
				geoproperty, geometryProperty, lang, scopeQ, localOnly, options, limit, offset, count, containedBy,
				join, joinLevel, doNotCompact, entityMapToken, entityMapRetrieve, pick, omit,
				format, jsonKeysQP, datasetId, distEntities, orderBy, orderFrom, orderGeometry, collation, metadata)
				.onItem()
				.transformToUni(qP -> {
					return queryService
							.query(tenant, qP.getEntityMapToken(), qP.isTokenProvided(),
									qP.getIdsAndTypeAndIdPattern(), qP.getAttrsQueryTerm(), qP.getqQueryTerm(),
									qP.getCsfQueryTerm(), qP.getGeoQueryTerm(), qP.getScopeQueryTerm(),
									qP.getLanguageQueryTerm(), qP.getLimit(), offset, count, qP.isLocalOnly(),
									qP.getContext(), request.headers(), doNotCompact, qP.getJsonKeys(),
									qP.getDataSetIdTerm(), join, joinLevel, distEntities, qP.getPickTerm(),
									qP.getOmitTerm(), qP.getCheckSum(), qP.getViaHeaders(), null, qP.getEntityMap(),
									qP.getOrderBy(), qP.isMetadata())
							.onItem().transform(qR -> Tuple5.of(qR, qP.getFinalOptions(), qP.getAcceptHeader(),
									qP.getLimit(), qP.getContext()));
				});

	}

	private Uni<Query> getQueryParam(HttpServerRequest request, String id, String typeQuery, String idPattern,
			String attrs, String q, String csf, String geometry, String georel, String coordinates,
			String geoproperty, String geometryProperty, String lang, String scopeQ, boolean localOnly, String options,
			Integer limit, int offset, boolean count, String containedBy, String join, int joinLevel,
			boolean doNotCompact, String entityMapToken, boolean entityMapRetrieve, String pick, String omit,
			String format, String jsonKeysQP, String datasetId,
			boolean distEntities, String orderBy, String orderFrom, String orderGeometry, String collation,
			boolean metadata) {

		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if ((pick != null && omit != null) || (pick != null && attrs != null) || (attrs != null && omit != null)) {
			return Uni.createFrom().failure(
					new ResponseException(ErrorType.BadRequestData, "Omit, pick and attrs are mutually exclusive"));
		}
		Set<String> finalOptions;
		try {
			finalOptions = HttpUtils.parseOptionsAndFormat(options, format);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		if (acceptHeader == -1) {
			return Uni.createFrom()
					.failure(new ResponseException(ErrorType.NotAcceptable, "Provided accept types are not supported"));
		}
		int actualLimit;
		if (limit == null) {
			actualLimit = defaultLimit;
		} else {
			actualLimit = limit;
		}
		if (actualLimit > maxLimit) {
			return Uni.createFrom().failure(new ResponseException(ErrorType.TooManyResults));
		}
		if (!localOnly && id == null && typeQuery == null && attrs == null && geometry == null && q == null
				&& pick == null) {
			return Uni.createFrom().failure(new ResponseException(ErrorType.InvalidRequest,
					"Minimum required input field is id or type or attrs or q or pick or a geo query"));
		}
		if (omit != null && pick != null) {
			return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData));
		}
		if (actualLimit == 0 && !count) {
			return Uni.createFrom().failure(new ResponseException(ErrorType.BadLimitQuery));
		}
		logger.debug("Query called: " + request.path());
		List<Object> headerContext;
		headerContext = HttpUtils.getAtContext(request);

		Set<String> jsonKeys = new HashSet<>();
		if (jsonKeysQP != null) {
			jsonKeys.addAll(Arrays.asList(jsonKeysQP.split(",")));
		}
		return HttpUtils.getContext(headerContext, ldService).onItem().transformToUni(context -> {
			AttrsQueryTerm attrsQuery;
			TypeQueryTerm typeQueryTerm;
			QQueryTerm qQueryTerm;
			CSFQueryTerm csfQueryTerm;
			GeoQueryTerm geoQueryTerm;
			ScopeQueryTerm scopeQueryTerm;
			LanguageQueryTerm langQuery;
			DataSetIdTerm dataSetIdTerm;
			OmitTerm omitTerm = null;
			PickTerm pickTerm = null;
			OrderByTerm orderByTerm;
			Query result = new Query();
			try {
				if (pick != null) {
					pickTerm = new PickTerm();
					QueryParser.parseProjectionTerm(pickTerm, pick, context);
				}
				if (omit != null) {
					omitTerm = OmitTerm.getNewRootInstance();
					QueryParser.parseProjectionTerm(omitTerm, omit, context);
				}

				attrsQuery = QueryParser.parseAttrs(attrs, context);
				dataSetIdTerm = QueryParser.parseDataSetId(datasetId);
				typeQueryTerm = QueryParser.parseTypeQuery(typeQuery, context);

				if (typeQueryTerm != null && typeQueryTerm.getAllTypes().contains(NGSIConstants.NGSI_LD_STAR)) {
					result.setLocalOnly(true);
					typeQueryTerm = null;
				} else {
					result.setLocalOnly(localOnly);
				}
				qQueryTerm = QueryParser.parseQuery(q, context);
				csfQueryTerm = QueryParser.parseCSFQuery(csf, context);
				geoQueryTerm = QueryParser.parseGeoQuery(georel, coordinates, geometry, geoproperty, context);
				scopeQueryTerm = QueryParser.parseScopeQuery(scopeQ);
				langQuery = QueryParser.parseLangQuery(lang);
				orderByTerm = QueryParser.parseOrderBy(orderBy, collation, orderFrom, orderGeometry, context);

			} catch (Exception e) {
				return Uni.createFrom().failure(e);
			}
			String[] ids;
			if (id != null) {
				List<String> tmpIds = Lists.newArrayList();
				int lastIdx = 0;
				int currentIdx = 0;
				String tmpId;
				while (true) {
					currentIdx = id.indexOf(',', lastIdx + 1);
					if (currentIdx == -1) {
						tmpId = id.substring(lastIdx);
						try {
							HttpUtils.validateUri(tmpId);
						} catch (Exception e) {
							return Uni.createFrom().failure(e);
						}
						tmpIds.add(tmpId);
						break;
					}
					tmpId = id.substring(lastIdx, currentIdx);
					try {
						HttpUtils.validateUri(tmpId);
					} catch (Exception e) {
						return Uni.createFrom().failure(e);
					}
					tmpIds.add(tmpId);
					lastIdx = currentIdx + 1;
				}

				ids = tmpIds.toArray(new String[0]);
			} else {
				ids = null;
			}
			String token;
			boolean tokenProvided;
			boolean forceEntitymapCreation;
			if (entityMapToken != null) {
				try {
					HttpUtils.validateUri(entityMapToken);
				} catch (ResponseException e) {
					return Uni.createFrom().failure(e);
				}
				token = entityMapToken;
				if (AppConstants.ENTITYMAP_IGNORE.equals(token)) {
					tokenProvided = false;
					forceEntitymapCreation = false;
				} else {
					tokenProvided = true;
					forceEntitymapCreation = entityMapRetrieve;
				}

			} else {
				token = "urn:ngsi-ld:entitymap:" + UUID.randomUUID().toString();
				tokenProvided = false;
				forceEntitymapCreation = entityMapRetrieve;
			}
			String checkSum;
			if (typeQuery == null && attrs == null && q == null && csf == null && geometry == null && georel == null
					&& coordinates == null && geoproperty == null && geometryProperty == null && scopeQ == null
					&& pick == null && omit == null) {
				checkSum = "";
			} else {
				checkSum = String.valueOf(Objects.hashCode(typeQuery, attrs, q, csf, geometry, georel, coordinates,
						geoproperty, geometryProperty, scopeQ, pick, omit));
			}
			String tenant = HttpUtils.getTenant(request);
			ViaHeaders viaHeaders;
			try {
				viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
						microServiceUtils.getSourceAlias(tenant));
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeQueryAndIdPattern;
			if (typeQueryTerm != null || ids != null || idPattern != null) {
				idsAndTypeQueryAndIdPattern = new ArrayList<>(1);
				idsAndTypeQueryAndIdPattern.add(Tuple3.of(ids, typeQueryTerm, idPattern));
			} else {
				idsAndTypeQueryAndIdPattern = null;
			}

			result.setEntityMapToken(token);
			result.setTokenProvided(tokenProvided);
			result.setIdsAndTypeAndIdPattern(idsAndTypeQueryAndIdPattern);
			result.setAttrsQueryTerm(attrsQuery);
			result.setqQueryTerm(qQueryTerm);
			result.setCsfQueryTerm(csfQueryTerm);
			result.setGeoQueryTerm(geoQueryTerm);
			result.setScopeQueryTerm(scopeQueryTerm);
			result.setLanguageQueryTerm(langQuery);
			result.setJsonKeys(jsonKeys);
			result.setContext(context);
			result.setPickTerm(pickTerm);
			result.setOmitTerm(omitTerm);
			result.setCheckSum(checkSum);
			result.setViaHeaders(viaHeaders);
			result.setFinalOptions(finalOptions);
			result.setLimit(actualLimit);
			result.setAcceptHeader(acceptHeader);
			result.setDataSetIdTerm(dataSetIdTerm);
			result.setEntityMap(forceEntitymapCreation);
			result.setOrderBy(orderByTerm);
			result.setMetadata(metadata);
			return Uni.createFrom().item(result);
		});
	}

}