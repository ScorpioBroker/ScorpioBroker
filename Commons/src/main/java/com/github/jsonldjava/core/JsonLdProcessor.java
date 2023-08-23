package com.github.jsonldjava.core;

import static com.github.jsonldjava.utils.Obj.newMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.github.jsonldjava.core.JsonLdError.Error;
import com.github.jsonldjava.impl.NQuadRDFParser;
import com.github.jsonldjava.impl.NQuadTripleCallback;
import com.google.common.collect.Lists;

import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.ext.web.client.WebClient;

/**
 * This class implements the <a href=
 * "http://json-ld.org/spec/latest/json-ld-api/#the-jsonldprocessor-interface" >
 * JsonLdProcessor interface</a>, except that it does not currently support
 * asynchronous processing, and hence does not return Promises, instead directly
 * returning the results.
 *
 * @author tristan
 *
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class JsonLdProcessor {

	private static Context coreContext = null;
	// private static boolean initialized = false;
	private static String coreContextUrl;

	static void init(String coreContextUrl, Context coreContext) {
		JsonLdProcessor.coreContext = coreContext;
		JsonLdProcessor.coreContextUrl = coreContextUrl;
	}
//	public synchronized static void init(ClientManager clientManager, WebClient webClient, String coreContextUrl) {
//		if (JsonLdProcessor.initialized) {
//			return;
//		}
//		JsonLdProcessor.initialized = true;
//		JsonLdProcessor.coreContextUrl = coreContextUrl;
//		clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
//			return client.preparedQuery("SELECT body FROM context WHERE id='" + AppConstants.INTERNAL_NULL_KEY + "'")
//					.execute().onItem().transform(rows -> {
//						return rows.iterator().next().getJsonObject(0).getMap();
//					});
//		}).onItem().transformToUni(coreContextMap -> {
//			return new Context(new JsonLdOptions(JsonLdOptions.JSON_LD_1_1)).parse(coreContextMap, false, webClient)
//					.onItem().transform(coreContext -> {
//						JsonLdProcessor.coreContext = coreContext;
//						JsonLdProcessor.coreContext.getTermDefinition("features").remove("@container");
//						JsonLdProcessor.coreContext.getInverse();
//						return null;
//					});
//			// this explicitly removes the features term from the core as it is a commonly
//			// used term and we don't need the geo json definition
//
//		}).await().indefinitely();
//
//	}

	static Context getCoreContextClone() {
		Context clone = coreContext.clone();
		clone.inverse = null;
		return clone;
	}

	/**
	 * Compacts the given input using the context according to the steps in the
	 * <a href="http://www.w3.org/TR/json-ld-api/#compaction-algorithm"> Compaction
	 * algorithm</a>.
	 *
	 * @param input   The input JSON-LD object.
	 * @param context The context object to use for the compaction algorithm.
	 * @param opts    The {@link JsonLdOptions} that are to be sent to the
	 *                compaction algorithm.
	 * @return The compacted JSON-LD document
	 * @throws JsonLdError       If there is an error while compacting.
	 * @throws ResponseException
	 */
	static Uni<Map<String, Object>> compact(Object input, Object context, JsonLdOptions opts, WebClient webClient) {

		return compact(input, context, null, opts, -1, webClient);

	}

	static Uni<Map<String, Object>> compact(Object input, Object context, Context activeCtx, JsonLdOptions opts,
			int endPoint, WebClient webClient) {
		return compact(input, context, activeCtx, opts, endPoint, null, null, webClient);
	}

	/**
	 * Compacts the given input using the context according to the steps in the
	 * <a href="http://www.w3.org/TR/json-ld-api/#compaction-algorithm"> Compaction
	 * algorithm</a>.
	 *
	 * @param input     The input JSON-LD object.
	 * @param context   The context object to use for the compaction algorithm.
	 * @param opts      The {@link JsonLdOptions} that are to be sent to the
	 *                  compaction algorithm.
	 * @param langQuery
	 * @param options
	 * @return The compacted JSON-LD document
	 * @throws JsonLdError       If there is an error while compacting.
	 * @throws ResponseException
	 */
	static Uni<Map<String, Object>> compact(Object input, Object context, Context activeCtx, JsonLdOptions opts,
			int endPoint, Set<String> options, LanguageQueryTerm langQuery, WebClient webClient) {
		// 1)
		// TODO: look into java futures/promises

		// 2-6) NOTE: these are all the same steps as in expand

		final Object expanded = input;// expand(null, input, opts, -1, false);// input;//
		// 7)
		// NGSIComment: No need to do this expanded items do contain @context
		/*
		 * if (context instanceof Map && ((Map<String, Object>)
		 * context).containsKey(JsonLdConsts.CONTEXT)) { context = ((Map<String,
		 * Object>) context).get(JsonLdConsts.CONTEXT); }
		 */

		// 8)

		Uni<Context> ctxUni;
		if (activeCtx == null) {
			activeCtx = coreContext.clone();
		}
		if (context != null) {
			ctxUni = activeCtx.parse(context, true, webClient);
		} else {
			ctxUni = Uni.createFrom().item(activeCtx);
		}
		return ctxUni.onItem().transformToUni(ctx -> {
			Object compacted = new JsonLdApi(opts).compact(ctx, null, expanded, opts.getCompactArrays(), endPoint,
					options, langQuery);

			// final step of Compaction Algorithm
			// TODO: SPEC: the result result is a NON EMPTY array,
			if (compacted instanceof List) {
				// if (((List<Object>) compacted).isEmpty()) {
				// compacted = newMap();
				// } else {
				final Map<String, Object> tmp = newMap();
				// TODO: SPEC: doesn't specify to use vocab = true here
				tmp.put(ctx.compactIri(JsonLdConsts.GRAPH, true), compacted);
				compacted = tmp;
				// }
			}
			Object contextTBU;
			if (context == null) {
				contextTBU = new ArrayList<Object>();
			} else {
				contextTBU = context;
			}
			if (compacted != null) {
				// TODO: figure out if we can make "@context" appear at the start of
				// the keySet
				if (!ctx.dontAddCoreContext()) {
					if (contextTBU instanceof List) {
						((List) contextTBU).add(coreContextUrl);
					} else {
						ArrayList<Object> temp = new ArrayList<Object>();
						temp.add(context);
						temp.add(coreContextUrl);
						contextTBU = temp;
					}
				}
				if ((context instanceof Map && !((Map<String, Object>) context).isEmpty())
						|| (context instanceof List && !((List<Object>) context).isEmpty())) {
					if (!(context instanceof List)) {
						((Map<String, Object>) compacted).put(JsonLdConsts.CONTEXT, Lists.newArrayList(context));
					} else {
						((Map<String, Object>) compacted).put(JsonLdConsts.CONTEXT, context);
					}
//				if (context instanceof List && ((List<Object>) context).size() == 1 && opts.getCompactArrays()) {
//					((Map<String, Object>) compacted).put(JsonLdConsts.CONTEXT, ((List<Object>) context).get(0));
//				} else {

//				}
				}
			}

			// 9)
			return Uni.createFrom().item((Map<String, Object>) compacted);
		});
	}

	/**
	 * Expands the given input according to the steps in the
	 * <a href="http://www.w3.org/TR/json-ld-api/#expansion-algorithm">Expansion
	 * algorithm</a>.
	 *
	 * @param input The input JSON-LD object.
	 * @param opts  The {@link JsonLdOptions} that are to be sent to the expansion
	 *              algorithm.
	 * @return The expanded JSON-LD document
	 * @throws JsonLdError       If there is an error while expanding.
	 * @throws ResponseException
	 */
	static Uni<List<Object>> expand(List<Object> contextLinks, Object input, JsonLdOptions opts, int payloadType,
			boolean atContextAllowed, WebClient webClient) {
		// 1)
		// TODO: look into java futures/promises

		// 2) TODO: better verification of DOMString IRI
		Uni<Object> inputUni;
		if (input instanceof String && ((String) input).contains(":")) {
			inputUni = opts.getDocumentLoader().loadDocument((String) input, webClient).onItem().transformToUni(tmp -> {
				Object realInput;
				try {
					realInput = tmp.getDocument();
				} catch (final Exception e) {
					return Uni.createFrom().failure(new JsonLdError(Error.LOADING_DOCUMENT_FAILED, e));
				}

				// if set the base in options should override the base iri in the
				// active context
				// thus only set this as the base iri if it's not already set in
				// options
				if (opts.getBase() == null) {
					opts.setBase((String) realInput);
				}
				return Uni.createFrom().item(realInput);
			});

		} else {
			inputUni = Uni.createFrom().item(input);
		}

		// 3)

		Uni<Context> activeCtx;
		if (contextLinks != null && !contextLinks.isEmpty()) {
			activeCtx = coreContext.clone().parse(contextLinks, true, webClient);
		} else {
			activeCtx = Uni.createFrom().item(coreContext.clone());
		}
		// 4)
		return Uni.combine().all().unis(activeCtx, inputUni).asTuple().onItem().transformToUni(tuple -> {
			Object myInput = tuple.getItem2();
			Context ctx = tuple.getItem1();
			if (opts.getExpandContext() != null) {
				Object exCtx = opts.getExpandContext();
				if (exCtx instanceof Map && ((Map<String, Object>) exCtx).containsKey(JsonLdConsts.CONTEXT)) {
					exCtx = ((Map<String, Object>) exCtx).get(JsonLdConsts.CONTEXT);
				}
				return ctx.parse(exCtx, true, webClient).onItem()
						.transformToUni(ctx2 -> expand(ctx2, myInput, opts, payloadType, atContextAllowed, webClient));
			} else {
				return expand(ctx, myInput, opts, payloadType, atContextAllowed, webClient);
			}
		});

	}

	static Uni<List<Object>> expand(Context activeCtx, Object input, JsonLdOptions opts, int payloadType,
			boolean atContextAllowed, WebClient webClient) {

		// 5)
		// TODO: add support for getting a context from HTTP when content-type
		// is set to a jsonld compatable format

		// 6)
		return new JsonLdApi(opts).expand(activeCtx, input, payloadType, atContextAllowed, webClient).onItem()
				.transform(expanded -> {
					// final step of Expansion Algorithm
					if (expanded instanceof Map && ((Map) expanded).containsKey(JsonLdConsts.GRAPH)
							&& ((Map) expanded).size() == 1) {
						expanded = ((Map<String, Object>) expanded).get(JsonLdConsts.GRAPH);
					} else if (expanded == null) {
						expanded = new ArrayList<Object>();
					}

					// normalize to an array
					if (!(expanded instanceof List)) {
						final List<Object> tmp = new ArrayList<Object>();
						tmp.add(expanded);
						expanded = tmp;
					}
					return (List<Object>) expanded;
				});
	}

	/**
	 * Expands the given input according to the steps in the
	 * <a href="http://www.w3.org/TR/json-ld-api/#expansion-algorithm">Expansion
	 * algorithm</a>, using the default {@link JsonLdOptions}.
	 *
	 * @param input The input JSON-LD object.
	 * @return The expanded JSON-LD document
	 * @throws JsonLdError       If there is an error while expanding.
	 * @throws ResponseException
	 */
	static Uni<List<Object>> expand(Object input, WebClient webClient) {
		return expand(new ArrayList<>(0), input, new JsonLdOptions(""), -1, true, webClient);
	}

	static Uni<Object> flatten(Object input, Object context, JsonLdOptions opts, WebClient webClient,
			String atContextUrl) {
		// 2-6) NOTE: these are all the same steps as in expand
		return expand(new ArrayList<>(0), input, opts, -1, true, webClient).onItem().transformToUni(expanded -> {
			// 7)
			Object myContext = context;
			if (context instanceof Map && ((Map<String, Object>) context).containsKey(JsonLdConsts.CONTEXT)) {
				myContext = ((Map<String, Object>) context).get(JsonLdConsts.CONTEXT);
			}
			// 8) NOTE: blank node generation variables are members of JsonLdApi
			// 9) NOTE: the next block is the Flattening Algorithm described in
			// http://json-ld.org/spec/latest/json-ld-api/#flattening-algorithm

			// 1)
			final Map<String, Object> nodeMap = newMap();
			nodeMap.put(JsonLdConsts.DEFAULT, newMap());
			// 2)
			new JsonLdApi(opts).generateNodeMap(expanded, nodeMap);
			// 3)
			final Map<String, Object> defaultGraph = (Map<String, Object>) nodeMap.remove(JsonLdConsts.DEFAULT);
			// 4)
			for (final String graphName : nodeMap.keySet()) {
				final Map<String, Object> graph = (Map<String, Object>) nodeMap.get(graphName);
				// 4.1+4.2)
				Map<String, Object> entry;
				if (!defaultGraph.containsKey(graphName)) {
					entry = newMap();
					entry.put(JsonLdConsts.ID, graphName);
					defaultGraph.put(graphName, entry);
				} else {
					entry = (Map<String, Object>) defaultGraph.get(graphName);
				}
				// 4.3)
				// TODO: SPEC doesn't specify that this should only be added if it
				// doesn't exists
				if (!entry.containsKey(JsonLdConsts.GRAPH)) {
					entry.put(JsonLdConsts.GRAPH, new ArrayList<Object>());
				}
				final List<String> keys = new ArrayList<String>(graph.keySet());
				Collections.sort(keys);
				for (final String id : keys) {
					final Map<String, Object> node = (Map<String, Object>) graph.get(id);
					if (!(node.containsKey(JsonLdConsts.ID) && node.size() == 1)) {
						((List<Object>) entry.get(JsonLdConsts.GRAPH)).add(node);
					}
				}

			}
			// 5)
			final List<Object> flattened = new ArrayList<Object>();
			// 6)
			final List<String> keys = new ArrayList<String>(defaultGraph.keySet());
			Collections.sort(keys);
			for (final String id : keys) {
				final Map<String, Object> node = (Map<String, Object>) defaultGraph.get(id);
				if (!(node.containsKey(JsonLdConsts.ID) && node.size() == 1)) {
					flattened.add(node);
				}
			}
			// 8)
			if (context != null && !flattened.isEmpty()) {
				Context activeCtx = new Context(atContextUrl, opts);
				return activeCtx.parse(context, false, webClient).onItem().transform(ctx -> {
					// TODO: only instantiate one jsonldapi
					Object compacted = new JsonLdApi(opts).compact(activeCtx, null, flattened, opts.getCompactArrays(),
							-1, null, null);
					if (!(compacted instanceof List)) {
						final List<Object> tmp = new ArrayList<Object>();
						tmp.add(compacted);
						compacted = tmp;
					}
					final String alias = activeCtx.compactIri(JsonLdConsts.GRAPH);
					final Map<String, Object> rval = activeCtx.serialize();
					rval.put(alias, compacted);
					return rval;
				});
			}
			return Uni.createFrom().item(flattened);
		});
	}

	/**
	 * Flattens the given input and compacts it using the passed context according
	 * to the steps in the
	 * <a href="http://www.w3.org/TR/json-ld-api/#flattening-algorithm"> Flattening
	 * algorithm</a>:
	 *
	 * @param input The input JSON-LD object.
	 * @param opts  The {@link JsonLdOptions} that are to be sent to the flattening
	 *              algorithm.
	 * @return The flattened JSON-LD document
	 * @throws JsonLdError       If there is an error while flattening.
	 * @throws ResponseException
	 */
	static Uni<Object> flatten(Object input, JsonLdOptions opts, WebClient webClient, String atContextUrl) {
		return flatten(input, null, opts, webClient, atContextUrl);
	}

	/**
	 * Frames the given input using the frame according to the steps in the
	 * <a href= "http://json-ld.org/spec/latest/json-ld-framing/#framing-algorithm">
	 * Framing Algorithm</a>.
	 *
	 * @param input The input JSON-LD object.
	 * @param frame The frame to use when re-arranging the data of input; either in
	 *              the form of an JSON object or as IRI.
	 * @param opts  The {@link JsonLdOptions} that are to be sent to the framing
	 *              algorithm.
	 * @return The framed JSON-LD document
	 * @throws JsonLdError       If there is an error while framing.
	 * @throws ResponseException
	 */
	static Uni<Map<String, Object>> frame(Object input, Object frame, JsonLdOptions opts, WebClient webClient,
			String atContextUrl) {
		Object myFrame;
		if (frame instanceof Map) {
			myFrame = JsonLdUtils.clone(frame);
		} else {
			myFrame = frame;
		}
		// TODO string/IO input

		// 2. Set expanded input to the result of using the expand method using
		// input and options.
		return expand(new ArrayList<>(0), input, opts, -1, true, webClient).onItem().transformToUni(expandedInput -> {

			// 3. Set expanded frame to the result of using the expand method using
			// frame and options with expandContext set to null and the
			// frameExpansion option set to true.
			final Object savedExpandedContext = opts.getExpandContext();
			opts.setExpandContext(null);
			opts.setFrameExpansion(true);
			return expand(new ArrayList<>(0), myFrame, opts, -1, true, webClient).onItem()
					.transformToUni(expandedFrame -> {
						opts.setExpandContext(savedExpandedContext);

						// 4. Set context to the value of @context from frame, if it exists, or
						// to a new empty
						// context, otherwise.
						final JsonLdApi api = new JsonLdApi(expandedInput, opts, atContextUrl);
						return api.context
								.parse(((Map<String, Object>) myFrame).get(JsonLdConsts.CONTEXT), false, webClient)
								.onItem().transform(activeCtx -> {
									final List<Object> framed = api.frame(expandedInput, expandedFrame);
									if (opts.getPruneBlankNodeIdentifiers()) {
										JsonLdUtils.pruneBlankNodes(framed);
									}
									Object compacted = api.compact(activeCtx, null, framed, opts.getCompactArrays(), -1,
											null, null);
									final Map<String, Object> rval = activeCtx.serialize();
									final boolean addGraph = ((!(compacted instanceof List)) && !opts.getOmitGraph());
									if (addGraph && !(compacted instanceof List)) {
										final List<Object> tmp = new ArrayList<Object>();
										tmp.add(compacted);
										compacted = tmp;
									}
									if (addGraph || (compacted instanceof List)) {
										final String alias = activeCtx.compactIri(JsonLdConsts.GRAPH);
										rval.put(alias, compacted);
									} else if (!addGraph && (compacted instanceof Map)) {
										rval.putAll((Map) compacted);
									}
									JsonLdUtils.removePreserve(activeCtx, rval, opts);
									return rval;
								});
					});
		});

	}

	/**
	 * A registry for RDF Parsers (in this case, JSONLDSerializers) used by fromRDF
	 * if no specific serializer is specified and options.format is set.
	 *
	 * TODO: this would fit better in the document loader class
	 */
	private static Map<String, RDFParser> rdfParsers = new LinkedHashMap<String, RDFParser>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 2642325778310758215L;

		{
			// automatically register nquad serializer
			put(JsonLdConsts.APPLICATION_NQUADS, new NQuadRDFParser());
		}
	};

	static void registerRDFParser(String format, RDFParser parser) {
		rdfParsers.put(format, parser);
	}

	static void removeRDFParser(String format) {
		rdfParsers.remove(format);
	}

	/**
	 * Converts an RDF dataset to JSON-LD.
	 *
	 * @param dataset a serialized string of RDF in a format specified by the format
	 *                option or an RDF dataset to convert.
	 * @param options the options to use: [format] the format if input is not an
	 *                array: 'application/nquads' for N-Quads (default).
	 *                [useRdfType] true to use rdf:type, false to use @type
	 *                (default: false). [useNativeTypes] true to convert XSD types
	 *                into native types (boolean, integer, double), false not to
	 *                (default: true).
	 * @return A JSON-LD object.
	 * @throws JsonLdError       If there is an error converting the dataset to
	 *                           JSON-LD.
	 * @throws ResponseException
	 */
	static Uni<Object> fromRDF(Object dataset, JsonLdOptions options, WebClient webClient, String atContextUrl) {
		// handle non specified serializer case

		RDFParser parser = null;

		if (options.format == null && dataset instanceof String) {
			// attempt to parse the input as nquads
			options.format = JsonLdConsts.APPLICATION_NQUADS;
		}

		if (rdfParsers.containsKey(options.format)) {
			parser = rdfParsers.get(options.format);
		} else {
			throw new JsonLdError(JsonLdError.Error.UNKNOWN_FORMAT, options.format);
		}

		// convert from RDF
		return fromRDF(dataset, options, parser, webClient, atContextUrl);
	}

	/**
	 * Converts an RDF dataset to JSON-LD, using the default {@link JsonLdOptions}.
	 *
	 * @param dataset a serialized string of RDF in a format specified by the format
	 *                option or an RDF dataset to convert.
	 * @return The JSON-LD object represented by the given RDF dataset
	 * @throws JsonLdError       If there was an error converting from RDF to
	 *                           JSON-LD
	 * 
	 * @throws ResponseException
	 */
	static Uni<Object> fromRDF(Object dataset, WebClient webClient, String atContextUrl) {
		return fromRDF(dataset, new JsonLdOptions(""), webClient, atContextUrl);
	}

	/**
	 * Converts an RDF dataset to JSON-LD, using a specific instance of
	 * {@link RDFParser}.
	 *
	 * @param input   a serialized string of RDF in a format specified by the format
	 *                option or an RDF dataset to convert.
	 * @param options the options to use: [format] the format if input is not an
	 *                array: 'application/nquads' for N-Quads (default).
	 *                [useRdfType] true to use rdf:type, false to use @type
	 *                (default: false). [useNativeTypes] true to convert XSD types
	 *                into native types (boolean, integer, double), false not to
	 *                (default: true).
	 * @param parser  A specific instance of {@link RDFParser} to use for the
	 *                conversion.
	 * @return A JSON-LD object.
	 * @throws JsonLdError       If there is an error converting the dataset to
	 *                           JSON-LD.
	 * @throws ResponseException
	 */
	static Uni<Object> fromRDF(Object input, JsonLdOptions options, RDFParser parser, WebClient webClient,
			String atContextUrl) {

		final RDFDataset dataset = parser.parse(input);

		// convert from RDF
		final Object rval = new JsonLdApi(options).fromRDF(dataset);

		// re-process using the generated context if outputForm is set
		if (options.outputForm != null) {
			if (JsonLdConsts.EXPANDED.equals(options.outputForm)) {
				Uni.createFrom().item(rval);
			} else if (JsonLdConsts.COMPACTED.equals(options.outputForm)) {
				return compact(rval, dataset.getContext(), options, webClient).onItem().transform(map -> (Object) map);
			} else if (JsonLdConsts.FLATTENED.equals(options.outputForm)) {
				return flatten(rval, dataset.getContext(), options, webClient, atContextUrl);
			} else {
				return Uni.createFrom().failure(new JsonLdError(JsonLdError.Error.UNKNOWN_ERROR,
						"Output form was unknown: " + options.outputForm));
			}
		}
		return Uni.createFrom().item(rval);
	}

	/**
	 * Converts an RDF dataset to JSON-LD, using a specific instance of
	 * {@link RDFParser}, and the default {@link JsonLdOptions}.
	 *
	 * @param input  a serialized string of RDF in a format specified by the format
	 *               option or an RDF dataset to convert.
	 * @param parser A specific instance of {@link RDFParser} to use for the
	 *               conversion.
	 * @return A JSON-LD object.
	 * @throws JsonLdError       If there is an error converting the dataset to
	 *                           JSON-LD.
	 * @throws ResponseException
	 */
	static Uni<Object> fromRDF(Object input, RDFParser parser, WebClient webClient, String atContextUrl) {
		return fromRDF(input, new JsonLdOptions(""), parser, webClient, atContextUrl);
	}

	/**
	 * Outputs the RDF dataset found in the given JSON-LD object.
	 *
	 * @param input    the JSON-LD input.
	 * @param callback A callback that is called when the input has been converted
	 *                 to Quads (null to use options.format instead).
	 * @param options  the options to use: [base] the base IRI to use. [format] the
	 *                 format to use to output a string: 'application/nquads' for
	 *                 N-Quads (default). [loadContext(url, callback(err, url,
	 *                 result))] the context loader.
	 * @return The result of executing {@link JsonLdTripleCallback#call(RDFDataset)}
	 *         on the results, or if {@link JsonLdOptions#format} is not null, a
	 *         result in that format if it is found, or otherwise the raw
	 *         {@link RDFDataset}.
	 * @throws JsonLdError       If there is an error converting the dataset to
	 *                           JSON-LD.
	 * @throws ResponseException
	 */
	static Uni<Object> toRDF(Object input, JsonLdTripleCallback callback, JsonLdOptions options, WebClient webClient,
			String atContextUrl) {
		return expand(new ArrayList<>(0), input, options, -1, true, webClient).onItem()
				.transformToUni(expandedInput -> {

					final JsonLdApi api = new JsonLdApi(expandedInput, options, atContextUrl);
					final RDFDataset dataset = api.toRDF();
					Uni<Void> uni = Uni.createFrom().voidItem();
					// generate namespaces from context
					if (options.useNamespaces) {
						List<Map<String, Object>> _input;
						if (input instanceof List) {
							_input = (List<Map<String, Object>>) input;
						} else {
							_input = new ArrayList<Map<String, Object>>();
							_input.add((Map<String, Object>) input);
						}

						for (final Map<String, Object> e : _input) {
							if (e.containsKey(JsonLdConsts.CONTEXT)) {
								uni = uni.onItem().transformToUni(v -> dataset.parseContext(e.get(JsonLdConsts.CONTEXT),
										webClient, atContextUrl));
							}
						}
					}
					return uni.onItem().transformToUni(v -> {
						if (callback != null) {
							return Uni.createFrom().item(callback.call(dataset));
						}

						if (options.format != null) {
							if (JsonLdConsts.APPLICATION_NQUADS.equals(options.format)) {
								return Uni.createFrom().item(new NQuadTripleCallback().call(dataset));
							} else {
								return Uni.createFrom()
										.failure(new JsonLdError(JsonLdError.Error.UNKNOWN_FORMAT, options.format));
							}
						}
						return Uni.createFrom().item(dataset);
					});
				});
	}

	/**
	 * Outputs the RDF dataset found in the given JSON-LD object.
	 *
	 * @param input   the JSON-LD input.
	 * @param options the options to use: [base] the base IRI to use. [format] the
	 *                format to use to output a string: 'application/nquads' for
	 *                N-Quads (default). [loadContext(url, callback(err, url,
	 *                result))] the context loader.
	 * @return A JSON-LD object.
	 * @throws JsonLdError       If there is an error converting the dataset to
	 *                           JSON-LD.
	 * @throws ResponseException
	 */
	static Uni<Object> toRDF(Object input, JsonLdOptions options, WebClient webClient, String atContextUrl) {
		return toRDF(input, null, options, webClient, atContextUrl);
	}

	/**
	 * Outputs the RDF dataset found in the given JSON-LD object, using the default
	 * {@link JsonLdOptions}.
	 *
	 * @param input    the JSON-LD input.
	 * @param callback A callback that is called when the input has been converted
	 *                 to Quads (null to use options.format instead).
	 * @return A JSON-LD object.
	 * @throws JsonLdError       If there is an error converting the dataset to
	 *                           JSON-LD.
	 * @throws ResponseException
	 */
	static Uni<Object> toRDF(Object input, JsonLdTripleCallback callback, WebClient webClient, String atContextUrl) {
		return toRDF(input, callback, new JsonLdOptions(""), webClient, atContextUrl);
	}

	/**
	 * Outputs the RDF dataset found in the given JSON-LD object, using the default
	 * {@link JsonLdOptions}.
	 *
	 * @param input the JSON-LD input.
	 * @return A JSON-LD object.
	 * @throws JsonLdError       If there is an error converting the dataset to
	 *                           JSON-LD.
	 * @throws ResponseException
	 */
	static Uni<Object> toRDF(Object input, WebClient webClient, String atContextUrl) {
		return toRDF(input, new JsonLdOptions(""), webClient, atContextUrl);
	}

	/**
	 * Performs RDF dataset normalization on the given JSON-LD input. The output is
	 * an RDF dataset unless the 'format' option is used.
	 *
	 * @param input   the JSON-LD input to normalize.
	 * @param options the options to use: [base] the base IRI to use. [format] the
	 *                format if output is a string: 'application/nquads' for
	 *                N-Quads. [loadContext(url, callback(err, url, result))] the
	 *                context loader.
	 * @return The JSON-LD object
	 * @throws JsonLdError       If there is an error normalizing the dataset.
	 * @throws ResponseException
	 */
	static Uni<Object> normalize(Object input, JsonLdOptions options, WebClient webClient, String atContextUrl) {

		final JsonLdOptions opts = options.copy();
		opts.format = null;
		return toRDF(input, opts, webClient, atContextUrl).onItem().transform(rdf -> {
			final RDFDataset dataset = (RDFDataset) rdf;

			return new JsonLdApi(options).normalize(dataset);
		});
	}

	/**
	 * Performs RDF dataset normalization on the given JSON-LD input. The output is
	 * an RDF dataset unless the 'format' option is used. Uses the default
	 * {@link JsonLdOptions}.
	 *
	 * @param input the JSON-LD input to normalize.
	 * @return The JSON-LD object
	 * @throws JsonLdError       If there is an error normalizing the dataset.
	 * @throws ResponseException
	 */
	static Uni<Object> normalize(Object input, WebClient webClient, String atContextUrl) {
		return normalize(input, new JsonLdOptions(""), webClient, atContextUrl);
	}

	static Context getCoreContext() {
		return coreContext;
	}

	public static Map<String, Object> compactWithLoadedContext(Object input, Object context, Context activeCtx,
			JsonLdOptions opts, int endPoint) {
		final Object expanded = input;// expand(null, input, opts, -1, false);// input;//

		Object compacted = new JsonLdApi(opts).compact(activeCtx, null, expanded, opts.getCompactArrays(), endPoint,
				null, null);

		// final step of Compaction Algorithm
		// TODO: SPEC: the result result is a NON EMPTY array,
		if (compacted instanceof List) {
			// if (((List<Object>) compacted).isEmpty()) {
			// compacted = newMap();
			// } else {
			final Map<String, Object> tmp = newMap();
			// TODO: SPEC: doesn't specify to use vocab = true here
			tmp.put(activeCtx.compactIri(JsonLdConsts.GRAPH, true), compacted);
			compacted = tmp;
			// }
		}
		Object contextTBU;
		if (context == null) {
			contextTBU = new ArrayList<Object>();
		} else {
			contextTBU = context;
		}
		if (compacted != null) {
			// TODO: figure out if we can make "@context" appear at the start of
			// the keySet
			if (!activeCtx.dontAddCoreContext()) {
				if (contextTBU instanceof List) {
					((List) contextTBU).add(coreContextUrl);
				} else {
					ArrayList<Object> temp = new ArrayList<Object>();
					temp.add(context);
					temp.add(coreContextUrl);
					contextTBU = temp;
				}
			}
			if ((context instanceof Map && !((Map<String, Object>) context).isEmpty())
					|| (context instanceof List && !((List<Object>) context).isEmpty())) {
				if (!(context instanceof List)) {
					((Map<String, Object>) compacted).put(JsonLdConsts.CONTEXT, Lists.newArrayList(context));
				} else {
					((Map<String, Object>) compacted).put(JsonLdConsts.CONTEXT, context);
				}
//				if (context instanceof List && ((List<Object>) context).size() == 1 && opts.getCompactArrays()) {
//					((Map<String, Object>) compacted).put(JsonLdConsts.CONTEXT, ((List<Object>) context).get(0));
//				} else {

//				}
			}
		}

		// 9)
		return (Map<String, Object>) compacted;

	}

}
