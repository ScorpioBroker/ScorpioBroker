package eu.neclab.ngsildbroker.commons.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.DeflaterOutputStream;

@Singleton
public class MicroServiceUtils {
	private final static Logger logger = LoggerFactory.getLogger(MicroServiceUtils.class);

	@ConfigProperty(name = "scorpio.gatewayurl")
	String gatewayUrl;

	@ConfigProperty(name = "mysettings.gateway.port")
	int port;
	@ConfigProperty(name = "atcontext.url", defaultValue = "http://localhost:9090/ngsi-ld/v1/jsonldContexts/")
	String contextServerUrl;

	private static final Encoder base64Encoder = Base64.getEncoder();

	@PostConstruct
	void setup() {
		if (contextServerUrl.endsWith("ngsi-ld/v1/jsonldContexts")) {
			contextServerUrl = contextServerUrl + "/";
		} else if (!contextServerUrl.endsWith("ngsi-ld/v1/jsonldContexts/")) {
			if (contextServerUrl.endsWith("/")) {
				contextServerUrl = contextServerUrl + "/ngsi-ld/v1/jsonldContexts/";
			} else {
				contextServerUrl = contextServerUrl + "ngsi-ld/v1/jsonldContexts/";
			}
		}
	}

	public static void putIntoIdMap(Map<String, List<Map<String, Object>>> localEntities, String id,
			Map<String, Object> local) {
		List<Map<String, Object>> tmp = localEntities.get(id);
		if (tmp == null) {
			tmp = Lists.newArrayList();
			localEntities.put(id, tmp);
		}
		tmp.add(local);
	}

	public static void main(String[] args) throws IOException {
		String data = "{\r\n" + "  \"@id\": \"urn:ngsi-ld:Mon.Container:1\",\r\n" + "  \"@type\": [\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/default-context/ccoc.mon.docker.container\"\r\n" + "  ],\r\n"
				+ "  \"https://uri.etsi.org/ngsi-ld/status\": [\r\n" + "    {\r\n" + "      \"@type\": [\r\n"
				+ "        \"https://uri.etsi.org/ngsi-ld/Property\"\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/hasValue\": [\r\n" + "        {\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/pid\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 27777\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/exitcode\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 0\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/oomkilled\": [\r\n" + "            {\r\n"
				+ "              \"@value\": false\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/uptime_ns\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 2055140123219736\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/started_at\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 1663341802691442000\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/container_status\": [\r\n"
				+ "            {\r\n" + "              \"@value\": \"running\"\r\n" + "            }\r\n"
				+ "          ]\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/createdAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/modifiedAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ]\r\n"
				+ "    }\r\n" + "  ],\r\n" + "  \"https://uri.etsi.org/ngsi-ld/createdAt\": [\r\n" + "    {\r\n"
				+ "      \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "      \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "    }\r\n" + "  ],\r\n"
				+ "  \"https://uri.etsi.org/ngsi-ld/modifiedAt\": [\r\n" + "    {\r\n"
				+ "      \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "      \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "    }\r\n" + "  ],\r\n"
				+ "  \"https://uri.etsi.org/ngsi-ld/observedAt\": [\r\n" + "    {\r\n" + "      \"@type\": [\r\n"
				+ "        \"https://uri.etsi.org/ngsi-ld/Property\"\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/hasValue\": [\r\n" + "        {\r\n"
				+ "          \"@value\": \"2022-10-10T10:15:45.000Z\"\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/createdAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/modifiedAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ]\r\n"
				+ "    }\r\n" + "  ],\r\n" + "  \"https://uri.etsi.org/ngsi-ld/default-context/cpu\": [\r\n"
				+ "    {\r\n" + "      \"@type\": [\r\n" + "        \"https://uri.etsi.org/ngsi-ld/Property\"\r\n"
				+ "      ],\r\n" + "      \"https://uri.etsi.org/ngsi-ld/hasValue\": [\r\n" + "        {\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/name\": [\r\n" + "            {\r\n"
				+ "              \"@value\": \"cpu0\"\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/usage_total\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 149510349149\r\n" + "            }\r\n" + "          ]\r\n"
				+ "        },\r\n" + "        {\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/name\": [\r\n" + "            {\r\n"
				+ "              \"@value\": \"cpu1\"\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/usage_total\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 150628581025\r\n" + "            }\r\n" + "          ]\r\n"
				+ "        },\r\n" + "        {\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/name\": [\r\n" + "            {\r\n"
				+ "              \"@value\": \"cpu2\"\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/usage_total\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 149062144967\r\n" + "            }\r\n" + "          ]\r\n"
				+ "        },\r\n" + "        {\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/name\": [\r\n" + "            {\r\n"
				+ "              \"@value\": \"cpu3\"\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/usage_total\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 119866812052\r\n" + "            }\r\n" + "          ]\r\n"
				+ "        },\r\n" + "        {\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/name\": [\r\n" + "            {\r\n"
				+ "              \"@value\": \"cpu4\"\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/usage_total\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 152174486019\r\n" + "            }\r\n" + "          ]\r\n"
				+ "        },\r\n" + "        {\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/name\": [\r\n" + "            {\r\n"
				+ "              \"@value\": \"cpu5\"\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/usage_total\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 149801986931\r\n" + "            }\r\n" + "          ]\r\n"
				+ "        },\r\n" + "        {\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/name\": [\r\n" + "            {\r\n"
				+ "              \"@value\": \"cpu6\"\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/usage_total\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 150837678672\r\n" + "            }\r\n" + "          ]\r\n"
				+ "        },\r\n" + "        {\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/name\": [\r\n" + "            {\r\n"
				+ "              \"@value\": \"cpu7\"\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/usage_total\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 152962823743\r\n" + "            }\r\n" + "          ]\r\n"
				+ "        }\r\n" + "      ],\r\n" + "      \"https://uri.etsi.org/ngsi-ld/createdAt\": [\r\n"
				+ "        {\r\n" + "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/modifiedAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ]\r\n"
				+ "    }\r\n" + "  ],\r\n" + "  \"https://uri.etsi.org/ngsi-ld/default-context/host\": [\r\n"
				+ "    {\r\n" + "      \"@type\": [\r\n" + "        \"https://uri.etsi.org/ngsi-ld/Relationship\"\r\n"
				+ "      ],\r\n" + "      \"https://uri.etsi.org/ngsi-ld/createdAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/hasObject\": [\r\n" + "        {\r\n"
				+ "          \"@id\": \"urn:ngsi-ld:Mon.Host:lab1worker2\"\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/modifiedAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ]\r\n"
				+ "    }\r\n" + "  ],\r\n" + "  \"https://uri.etsi.org/ngsi-ld/default-context/name\": [\r\n"
				+ "    {\r\n" + "      \"@type\": [\r\n" + "        \"https://uri.etsi.org/ngsi-ld/Property\"\r\n"
				+ "      ],\r\n" + "      \"https://uri.etsi.org/ngsi-ld/hasValue\": [\r\n" + "        {\r\n"
				+ "          \"@value\": \"k8s_postgres_postgres-68ff7ccc59-fk4wc_ccoc_31d4e8a5-9556-4009-b8d7-4ba14995895e_0\"\r\n"
				+ "        }\r\n" + "      ],\r\n" + "      \"https://uri.etsi.org/ngsi-ld/createdAt\": [\r\n"
				+ "        {\r\n" + "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/modifiedAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ]\r\n"
				+ "    }\r\n" + "  ],\r\n" + "  \"https://uri.etsi.org/ngsi-ld/default-context/image\": [\r\n"
				+ "    {\r\n" + "      \"@type\": [\r\n" + "        \"https://uri.etsi.org/ngsi-ld/Property\"\r\n"
				+ "      ],\r\n" + "      \"https://uri.etsi.org/ngsi-ld/hasValue\": [\r\n" + "        {\r\n"
				+ "          \"@value\": \"sha256\"\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/createdAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/modifiedAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ]\r\n"
				+ "    }\r\n" + "  ],\r\n" + "  \"https://uri.etsi.org/ngsi-ld/default-context/k8s_id\": [\r\n"
				+ "    {\r\n" + "      \"@type\": [\r\n" + "        \"https://uri.etsi.org/ngsi-ld/Property\"\r\n"
				+ "      ],\r\n" + "      \"https://uri.etsi.org/ngsi-ld/hasValue\": [\r\n" + "        {\r\n"
				+ "          \"@value\": \"postgres\"\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/createdAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/modifiedAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ]\r\n"
				+ "    }\r\n" + "  ],\r\n" + "  \"https://uri.etsi.org/ngsi-ld/default-context/memory\": [\r\n"
				+ "    {\r\n" + "      \"@type\": [\r\n" + "        \"https://uri.etsi.org/ngsi-ld/Property\"\r\n"
				+ "      ],\r\n" + "      \"https://uri.etsi.org/ngsi-ld/hasValue\": [\r\n" + "        {\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/rss\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 3170304\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/cache\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 32673792\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/limit\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 4294967296\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/usage\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 16711680\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/pgpgin\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 69551922\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/pgfault\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 111059949\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/pgpgout\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 69543296\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/rss_huge\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 0\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/max_usage\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 114823168\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/total_rss\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 3170304\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/writeback\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 0\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/pgmajfault\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 66\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/active_anon\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 12206080\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/active_file\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 11251712\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/mapped_file\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 23113728\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/total_cache\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 32673792\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/unevictable\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 0\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/total_pgpgin\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 69551922\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/inactive_anon\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 11558912\r\n" + "            }\r\n"
				+ "          ],\r\n" + "          \"https://uri.etsi.org/ngsi-ld/default-context/inactive_file\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 593920\r\n" + "            }\r\n"
				+ "          ],\r\n" + "          \"https://uri.etsi.org/ngsi-ld/default-context/total_pgfault\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 111059949\r\n" + "            }\r\n"
				+ "          ],\r\n" + "          \"https://uri.etsi.org/ngsi-ld/default-context/total_pgpgout\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 69543296\r\n" + "            }\r\n"
				+ "          ],\r\n" + "          \"https://uri.etsi.org/ngsi-ld/default-context/usage_percent\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 0.38909912109375\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/total_rss_huge\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 0\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/total_writeback\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 0\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/total_pgmajfault\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 66\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/total_active_anon\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 12206080\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/total_active_file\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 11251712\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/total_mapped_file\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 23113728\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/total_unevictable\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 0\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/total_inactive_anon\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 11558912\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/total_inactive_file\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 593920\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/hierarchical_memory_limit\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 4294967296\r\n" + "            }\r\n"
				+ "          ]\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/createdAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/modifiedAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ]\r\n"
				+ "    }\r\n" + "  ],\r\n" + "  \"https://uri.etsi.org/ngsi-ld/default-context/k8s_pod\": [\r\n"
				+ "    {\r\n" + "      \"@type\": [\r\n" + "        \"https://uri.etsi.org/ngsi-ld/Relationship\"\r\n"
				+ "      ],\r\n" + "      \"https://uri.etsi.org/ngsi-ld/createdAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/hasObject\": [\r\n" + "        {\r\n"
				+ "          \"@id\": \"urn:ngsi-ld:Mon.Pod:postgres-68ff7ccc59-fk4wc\"\r\n" + "        }\r\n"
				+ "      ],\r\n" + "      \"https://uri.etsi.org/ngsi-ld/modifiedAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ]\r\n"
				+ "    }\r\n" + "  ],\r\n" + "  \"https://uri.etsi.org/ngsi-ld/default-context/network\": [\r\n"
				+ "    {\r\n" + "      \"@type\": [\r\n" + "        \"https://uri.etsi.org/ngsi-ld/Property\"\r\n"
				+ "      ],\r\n" + "      \"https://uri.etsi.org/ngsi-ld/hasValue\": [],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/createdAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/modifiedAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ]\r\n"
				+ "    }\r\n" + "  ],\r\n" + "  \"https://uri.etsi.org/ngsi-ld/default-context/storage\": [\r\n"
				+ "    {\r\n" + "      \"@type\": [\r\n" + "        \"https://uri.etsi.org/ngsi-ld/Property\"\r\n"
				+ "      ],\r\n" + "      \"https://uri.etsi.org/ngsi-ld/hasValue\": [\r\n" + "        {\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/name\": [\r\n" + "            {\r\n"
				+ "              \"@value\": \"8:32\"\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_time_recursive\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 1997491130511\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_wait_time_read\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 833752867\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_wait_time_sync\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 951680504\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/sectors_recursive\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 3575144\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_wait_time_async\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 33868841906\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_wait_time_total\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 34820522410\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_wait_time_write\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 33986769543\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_queue_recursive_read\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 0\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_queue_recursive_sync\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 0\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_merged_recursive_read\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 46\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_merged_recursive_sync\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 46\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_queue_recursive_async\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 0\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_queue_recursive_total\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 0\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_queue_recursive_write\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 0\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_merged_recursive_async\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 1073\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_merged_recursive_total\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 1119\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_merged_recursive_write\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 1073\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_serviced_recursive_read\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 1357\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_serviced_recursive_sync\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 1384\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_serviced_recursive_async\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 292692\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_serviced_recursive_total\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 294076\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_serviced_recursive_write\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 292719\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_service_time_recursive_read\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 3620324320\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_service_time_recursive_sync\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 3645591282\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_service_bytes_recursive_read\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 26079232\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_service_bytes_recursive_sync\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 26230784\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_service_time_recursive_async\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 328985916690\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_service_time_recursive_total\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 332631507972\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_service_time_recursive_write\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 329011183652\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_service_bytes_recursive_async\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 1804242944\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_service_bytes_recursive_total\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 1830473728\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/io_service_bytes_recursive_write\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 1804394496\r\n" + "            }\r\n"
				+ "          ]\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/createdAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/modifiedAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ]\r\n"
				+ "    }\r\n" + "  ],\r\n" + "  \"https://uri.etsi.org/ngsi-ld/default-context/cpu_total\": [\r\n"
				+ "    {\r\n" + "      \"@type\": [\r\n" + "        \"https://uri.etsi.org/ngsi-ld/Property\"\r\n"
				+ "      ],\r\n" + "      \"https://uri.etsi.org/ngsi-ld/hasValue\": [\r\n" + "        {\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/usage_total\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 1174844862558\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/usage_system\": [\r\n" + "            {\r\n"
				+ "              \"@value\": 16234260960000000\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/usage_percent\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 0\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/usage_in_usermode\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 567440000000\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/throttling_periods\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 0\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/usage_in_kernelmode\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 559870000000\r\n" + "            }\r\n"
				+ "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/throttling_throttled_time\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 0\r\n" + "            }\r\n" + "          ],\r\n"
				+ "          \"https://uri.etsi.org/ngsi-ld/default-context/throttling_throttled_periods\": [\r\n"
				+ "            {\r\n" + "              \"@value\": 0\r\n" + "            }\r\n" + "          ]\r\n"
				+ "        }\r\n" + "      ],\r\n" + "      \"https://uri.etsi.org/ngsi-ld/createdAt\": [\r\n"
				+ "        {\r\n" + "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/modifiedAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ]\r\n"
				+ "    }\r\n" + "  ],\r\n" + "  \"https://uri.etsi.org/ngsi-ld/default-context/k8s_namespace\": [\r\n"
				+ "    {\r\n" + "      \"@type\": [\r\n" + "        \"https://uri.etsi.org/ngsi-ld/Property\"\r\n"
				+ "      ],\r\n" + "      \"https://uri.etsi.org/ngsi-ld/hasValue\": [\r\n" + "        {\r\n"
				+ "          \"@value\": \"ccoc\"\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/createdAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ],\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/modifiedAt\": [\r\n" + "        {\r\n"
				+ "          \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "          \"@value\": \"2024-08-08T09:38:05.201000Z\"\r\n" + "        }\r\n" + "      ]\r\n"
				+ "    }\r\n" + "  ]\r\n" + "}";
		long time1;
		long time2;
		long timel;
		long avg = 0;
		for (int i = 0; i < 2000; i++) {
			time1 = System.nanoTime();
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			// GZIPOutputStream zip = new GZIPOutputStream(byteArrayOutputStream);
			DeflaterOutputStream zip = new DeflaterOutputStream(byteArrayOutputStream);
			// ZipOutputStream zip = new ZipOutputStream(byteArrayOutputStream);
			// zip.setLevel(6);
			// zip.putNextEntry(new ZipEntry("dummy"));
			zip.write(data.getBytes());
			zip.flush();
			zip.close();
			time2 = System.nanoTime();
			timel = time2 - time1;
			// byteArrayOutputStream.flush();
			byte[] tmp = byteArrayOutputStream.toByteArray();
			avg += timel;
			System.out.println(tmp.length);
			System.out.println(timel);
		}
		System.out.println(avg / 2000);

	}

	public static void serializeAndSplitObjectAndEmit(Object obj, int maxMessageSize, MutinyEmitter<String> emitter,
			ObjectMapper objectMapper) throws ResponseException {
		if (obj instanceof BaseRequest br) {
			String base;
			try {
				base = objectMapper.writeValueAsString(br);
			} catch (JsonProcessingException e) {
				logger.error("Failed to serialize object", e);
				throw new ResponseException(ErrorType.InternalError, "Failed to serialize object");
			}
			logger.debug("attempting to send request with max message size " + maxMessageSize);
			base = base.substring(0, base.length() - 1);
			base += ",\"" + AppConstants.PAYLOAD_SERIALIZATION_CHAR + "\":[";
			String current = base;
			Map<String, List<Map<String, Object>>> payload = br.getPayload();
			Map<String, List<Map<String, Object>>> prevPayload = br.getPrevPayload();
			boolean zip = br.isZipped();
			List<String> toSend = Lists.newArrayList();
			if (payload != null) {
				boolean first = true;
				for (Entry<String, List<Map<String, Object>>> entry : payload.entrySet()) {
					String serializedPayload;
					String serializedPrevpayload;
					String id = entry.getKey();
					for (int i = 0; i < entry.getValue().size(); i++) {
						try {
							serializedPayload = objectMapper.writeValueAsString(entry.getValue().get(i));
						} catch (JsonProcessingException e) {
							logger.error("Failed to serialize object", e);
							throw new ResponseException(ErrorType.InternalError, "Failed to serialize object");
						}
						if (prevPayload != null) {
							List<Map<String, Object>> prev = prevPayload.get(id);
							if (prev != null) {
								if (i < prev.size()) {
									Map<String, Object> prevValue = prev.get(i);
									try {
										serializedPrevpayload = objectMapper.writeValueAsString(prevValue);
									} catch (JsonProcessingException e) {
										logger.error("Failed to serialize object", e);
										throw new ResponseException(ErrorType.InternalError,
												"Failed to serialize object");
									}
								} else {
									serializedPrevpayload = "null";
								}
							} else {
								serializedPrevpayload = "null";
							}
						} else {
							serializedPrevpayload = "null";
						}

						if (zip) {

							try {
								serializedPayload = base64Encoder.encodeToString(zip(serializedPayload));
							} catch (IOException e) {
								throw new ResponseException(ErrorType.InternalError, "Failed to compress prevpayload");
							}
							try {
								serializedPrevpayload = base64Encoder.encodeToString(zip(serializedPrevpayload));
							} catch (IOException e) {
								throw new ResponseException(ErrorType.InternalError, "Failed to compress prevpayload");
							}
						}
						int messageLength = current.getBytes().length + id.getBytes().length + serializedPayload.getBytes().length
								+ serializedPrevpayload.getBytes().length + 18;
						logger.debug("message size after adding payload would be " + maxMessageSize);
						if (messageLength > maxMessageSize) {
							if (first) {
								throw new ResponseException(ErrorType.RequestEntityTooLarge);
							}
							logger.debug("finalizing message");
							current = current.substring(0, current.length() - 1) + "]}";
							logger.debug("finale messagesize: " + current.length());
							toSend.add(current);
							current = base + "\"" + id + "\",";
							if (zip) {
								current += "\"";
							}
							current += serializedPayload;
							if (zip) {
								current += "\"";
							}
							current += ",";
							if (zip) {
								current += "\"";
							}
							current += serializedPrevpayload;
							if (zip) {
								current += "\"";
							}
							current += ",";
							first = true;
						} else if (messageLength == maxMessageSize) {
							logger.debug("finalizing message");
							current = current.substring(0, current.length() - 1) + "]}";
							logger.debug("finale messagesize: " + current.length());
							toSend.add(current);
							current = base;
							first = true;
						} else {
							current += "\"" + id + "\",";
							if (zip) {
								current += "\"";
							}
							current += serializedPayload;
							if (zip) {
								current += "\"";
							}
							current += ",";
							if (zip) {
								current += "\"";
							}
							current += serializedPrevpayload;
							if (zip) {
								current += "\"";
							}
							current += ",";
						}
					}
					first = false;
				}
				if (current.length() != base.length()) {
					logger.debug("finalizing message");
					current = current.substring(0, current.length() - 1) + "]}";
					logger.debug("finale messagesize: " + current.length());
					toSend.add(current);
					current = base;
				}
			} else if (prevPayload != null) {
				boolean first = true;
				for (Entry<String, List<Map<String, Object>>> entry : prevPayload.entrySet()) {
					String serializedPayload = "null";
					String serializedPrevpayload;
					String id = entry.getKey();
					for (Map<String, Object> mapEntry : entry.getValue()) {
						try {
							serializedPrevpayload = objectMapper.writeValueAsString(mapEntry);
						} catch (JsonProcessingException e) {
							logger.error("Failed to serialize object", e);
							throw new ResponseException(ErrorType.InternalError, "Failed to serialize object");
						}

						if (zip) {

							try {
								serializedPayload = base64Encoder.encodeToString(zip(serializedPayload));
							} catch (IOException e) {
								throw new ResponseException(ErrorType.InternalError, "Failed to compress prevpayload");
							}
							try {
								serializedPrevpayload = base64Encoder.encodeToString(zip(serializedPrevpayload));
							} catch (IOException e) {
								throw new ResponseException(ErrorType.InternalError, "Failed to compress prevpayload");
							}
						}
						int messageLength = current.getBytes().length + id.getBytes().length + serializedPayload.getBytes().length
								+ serializedPrevpayload.getBytes().length + 18;
						logger.debug("message size after adding payload would be " + maxMessageSize);
						if (messageLength > maxMessageSize) {
							if (first) {
								throw new ResponseException(ErrorType.RequestEntityTooLarge);
							}
							logger.debug("finalizing message only prevpayload");
							current = current.substring(0, current.length() - 1) + "]}";
							logger.debug("finale messagesize only prevpayload: " + current.length());
							toSend.add(current);
							current = base + "\"" + id + "\",";
							if (zip) {
								current += "\"";
							}
							current += serializedPayload;
							if (zip) {
								current += "\"";
							}
							current += ",";
							if (zip) {
								current += "\"";
							}
							current += serializedPrevpayload;
							if (zip) {
								current += "\"";
							}
							current += ",";
							first = true;
						} else if (messageLength == maxMessageSize) {
							logger.debug("finalizing message only prevpayload");
							current = current.substring(0, current.length() - 1) + "]}";
							logger.debug("finale messagesize only prevpayload: " + current.length());
							toSend.add(current);
							current = base;
							first = true;
						} else {
							current += "\"" + id + "\",";
							if (zip) {
								current += "\"";
							}
							current += serializedPayload;
							if (zip) {
								current += "\"";
							}
							current += ",";
							if (zip) {
								current += "\"";
							}
							current += serializedPrevpayload;
							if (zip) {
								current += "\"";
							}
							current += ",";
						}
					}
					first = false;
				}
				if (current.length() != base.length()) {
					logger.debug("finalizing message only prevpayload");
					current = current.substring(0, current.length() - 1) + "]}";
					logger.debug("finale messagesize only prevpayload: " + current.length());
					toSend.add(current);
					current = base;
				}
			} else if (br.getIds() != null) {
				boolean first = true;
				for (String entry : br.getIds()) {
					String serializedPayload = "null";
					String serializedPrevpayload = "null";
					String id = entry;

					if (zip) {

						try {
							serializedPayload = base64Encoder.encodeToString(zip(serializedPayload));
						} catch (IOException e) {
							throw new ResponseException(ErrorType.InternalError, "Failed to compress prevpayload");
						}
						try {
							serializedPrevpayload = base64Encoder.encodeToString(zip(serializedPrevpayload));
						} catch (IOException e) {
							throw new ResponseException(ErrorType.InternalError, "Failed to compress prevpayload");
						}
					}
					int messageLength = current.getBytes().length + id.getBytes().length + serializedPayload.getBytes().length
							+ serializedPrevpayload.getBytes().length + 18;
					logger.debug("message size after adding payload would be " + maxMessageSize);
					if (messageLength > maxMessageSize) {
						if (first) {
							throw new ResponseException(ErrorType.RequestEntityTooLarge);
						}
						logger.debug("finalizing message only ids");
						current = current.substring(0, current.length() - 1) + "]}";
						logger.debug("finale messagesize only ids: " + current.length());
						toSend.add(current);
						current = base + "\"" + id + "\",";
						if (zip) {
							current += "\"";
						}
						current += serializedPayload;
						if (zip) {
							current += "\"";
						}
						current += ",";
						if (zip) {
							current += "\"";
						}
						current += serializedPrevpayload;
						if (zip) {
							current += "\"";
						}
						current += ",";
						first = true;
					} else if (messageLength == maxMessageSize) {
						logger.debug("finalizing message only ids");
						current = current.substring(0, current.length() - 1) + "]}";
						logger.debug("finale messagesize only ids: " + current.length());
						toSend.add(current);
						current = base;
						first = true;
					} else {
						current += "\"" + id + "\",";
						if (zip) {
							current += "\"";
						}
						current += serializedPayload;
						if (zip) {
							current += "\"";
						}
						current += ",";
						if (zip) {
							current += "\"";
						}
						current += serializedPrevpayload;
						if (zip) {
							current += "\"";
						}
						current += ",";
					}
					first = false;
				}
				if (current.length() != base.length()) {
					logger.debug("finalizing message only ids");
					current = current.substring(0, current.length() - 1) + "]}";
					logger.debug("finale messagesize only ids: " + current.length());
					toSend.add(current);
					current = base;
				}
			} else {
				throw new ResponseException(ErrorType.InternalError, "Failed to compress prevpayload");
			}
			toSend.forEach(entry -> {
				logger.debug("sending entry of size: " + entry.length());
				emitter.sendAndForget(entry);
			});

		} else {
			String data;
			try {
				data = objectMapper.writeValueAsString(obj);
			} catch (JsonProcessingException e) {
				throw new ResponseException(ErrorType.InternalError, "Failed to serialize object");
			}
			emitter.sendAndForget(data);
		}

	}

	private static byte[] zip(String data) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		DeflaterOutputStream deflateOut = new DeflaterOutputStream(byteArrayOutputStream);
		deflateOut.write(data.getBytes());
		deflateOut.flush();
		deflateOut.close();
		byte[] tmp = byteArrayOutputStream.toByteArray();
		byteArrayOutputStream.close();
		return tmp;
	}

	public static List<String> splitStringByByteLength(String src, int maxsize) {
		String id = String.format("%020d", src.hashCode() * System.currentTimeMillis());
		logger.debug("Splitting into size " + maxsize);
		logger.debug(src);
		Charset cs = Charset.forName("UTF-16");
		CharsetEncoder coder = cs.newEncoder();
		ByteBuffer out = ByteBuffer.allocate(maxsize); // output buffer of required size
		CharBuffer in = CharBuffer.wrap(src);
		List<String> result = new ArrayList<>(); // a list to store the chunks
		int pos = 0;
		int i = 0;
		while (true) {
			CoderResult cr = coder.encode(in, out, true); // try to encode as much as possible
			int newpos = src.length() - in.length();
			String posS = String.format("%011d", i);
			String s = "$" + id + posS + src.substring(pos, newpos);
			i++;
			result.add(s); // add what has been encoded to the list
			pos = newpos; // store new input position
			out.rewind(); // and rewind output buffer
			if (!cr.isOverflow()) {
				break; // everything has been encoded
			}
		}
		result.set(0, "#" + id + String.format("%011d", i) + result.get(0).substring(32));
		result.set(result.size() - 1, "%" + result.get(result.size() - 1).substring(1));
		return result;
	}

	public URI getGatewayURL() {
		logger.trace("getGatewayURL() :: started");
		String url = null;
		try {
			if (gatewayUrl == null || gatewayUrl.strip().isEmpty()) {
				String hostIP = InetAddress.getLocalHost().getHostName();
				url = new StringBuilder("http://").append(hostIP).append(":").append(port).toString();
			} else {
				url = gatewayUrl;
			}
			logger.trace("getGatewayURL() :: completed");

			return new URI(url.toString());
		} catch (URISyntaxException | UnknownHostException e) {
			throw new AssertionError(
					"something went really wrong here when creating a URL... this should never happen but did with "
							+ url,
					e);
		}
	}

	public static Map<String, List<Map<String, Object>>> deepCopyIdMap(
			Map<String, List<Map<String, Object>>> original) {
		Map<String, List<Map<String, Object>>> result = new HashMap<>(original.size());
		original.forEach((key, value) -> {
			List<Map<String, Object>> tmp = new ArrayList<>(value.size());
			value.forEach(map -> {
				tmp.add(deepCopyMap(map));
			});
			result.put(key, tmp);
		});
		return result;
	}

	public static Map<String, Object> deepCopyMap(Map<String, Object> original) {
		if (original == null) {
			return null;
		}
		Map<String, Object> result = Maps.newHashMap();
		for (Entry<String, Object> entry : original.entrySet()) {
			Object copiedValue;
			Object originalValue = entry.getValue();
			if (originalValue instanceof List) {
				copiedValue = deppCopyList((List<Object>) originalValue);
			} else if (originalValue instanceof Map) {
				copiedValue = deepCopyMap((Map<String, Object>) originalValue);
			} else if (originalValue instanceof Integer) {
				copiedValue = ((Integer) originalValue).intValue();
			} else if (originalValue instanceof Long) {
				copiedValue = ((Long) originalValue).longValue();
			} else if (originalValue instanceof Float) {
				copiedValue = ((Float) originalValue).floatValue();
			} else if (originalValue instanceof Double) {
				copiedValue = ((Double) originalValue).doubleValue();
			} else if (originalValue instanceof Boolean) {
				copiedValue = ((Boolean) originalValue).booleanValue();
			} else if (originalValue == null) {
				continue;
			} else {
				copiedValue = originalValue.toString();
			}
			result.put(entry.getKey(), copiedValue);
		}

		return result;
	}

	private static List<Object> deppCopyList(List<Object> original) {
		if (original == null) {
			return null;
		}
		List<Object> result = Lists.newArrayList();
		for (Object originalValue : original) {
			Object copiedValue;
			if (originalValue instanceof List) {
				copiedValue = deppCopyList((List<Object>) originalValue);
			} else if (originalValue instanceof Map) {
				copiedValue = deepCopyMap((Map<String, Object>) originalValue);
			} else if (originalValue instanceof Integer) {
				copiedValue = ((Integer) originalValue).intValue();
			} else if (originalValue instanceof Double) {
				copiedValue = ((Double) originalValue).doubleValue();
			} else if (originalValue instanceof Float) {
				copiedValue = ((Float) originalValue).floatValue();
			} else if (originalValue instanceof Boolean) {
				copiedValue = ((Boolean) originalValue).booleanValue();
			} else {
				copiedValue = originalValue.toString();
			}
			result.add(copiedValue);
		}
		return result;
	}

	public static SubscriptionRequest deepCopySubscriptionMessage(SubscriptionRequest originalPayload) {
		SubscriptionRequest result = new SubscriptionRequest();
		result.setContext(originalPayload.getContext());
		result.setId(originalPayload.getId());
		if (originalPayload.getPayload() != null) {
			result.setPayload(deepCopyMap(originalPayload.getPayload()));
		}
		result.setTenant(originalPayload.getTenant());
		result.setRequestType(originalPayload.getRequestType());
		return result;
	}

	public static HeadersMultiMap getHeaders(ArrayListMultimap<String, String> receiverInfo) {
		HeadersMultiMap result = new HeadersMultiMap();
		for (Entry<String, String> entry : receiverInfo.entries()) {
			result.add(entry.getKey(), entry.getValue());
		}
		return result;
	}

//	public static SyncMessage deepCopySyncMessage(SyncMessage originalSync) {
//		SubscriptionRequest tmp = new SubscriptionRequest();
//		SubscriptionRequest originalPayload = originalSync.getRequest();
//		tmp.setActive(originalPayload.isActive());
//		tmp.setContext(deppCopyList(originalPayload.getContext()));
//		tmp.setPayload(deepCopyMap(originalPayload.getPayload()));
//		tmp.setHeaders(ArrayListMultimap.create(originalPayload.getHeaders()));
//		tmp.setId(originalPayload.getId());
//		tmp.setType(originalPayload.getRequestType());
//		tmp.setSubscription(new Subscription(originalPayload.getSubscription()));
//		return new SyncMessage(originalSync.getSyncId(), tmp);
//	}
	public URI getContextServerURL() {
		logger.trace("getContextServerURL :: started");
		try {
			return new URI(contextServerUrl);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}

	}
}
