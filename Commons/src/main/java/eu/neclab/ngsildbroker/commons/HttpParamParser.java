package eu.neclab.ngsildbroker.commons;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class HttpParamParser {

    /**
     * Entry point: parse the raw HTTP-param value.
     * Returns a single value or a List of values if commas are present.
     */
    public static Object parseParam(String raw) {
        List<String> tokens = splitValues(raw);
        if (tokens.size() == 1) {
            return parseSingle(tokens.get(0));
        }
        return tokens.stream()
                .map(HttpParamParser::parseSingle)
                .collect(Collectors.toList());
    }

    /**
     * Splits on commas that are not inside double quotes.
     * Preserves quotes and any escaped characters.
     */
    private static List<String> splitValues(String input) {
        List<String> values = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);

            if (c == '"' && (i == 0 || input.charAt(i - 1) != '\\')) {
                // Toggle quote state
                inQuotes = !inQuotes;
                sb.append(c);
            } else if (c == ',' && !inQuotes) {
                // Found comma outside quotes → split
                values.add(sb.toString().trim());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }

        // Add last token
        if (sb.length() > 0) {
            values.add(sb.toString().trim());
        }

        return values;
    }

    /**
     * Infers the type of a single token.
     * - Strips quotes if present (unescaping any '\"' inside)
     * - Tries boolean
     * - Tries integer, then double
     * - Falls back to raw unquoted string
     */
    private static String parseSingle(String token) {
        // Quoted string?
        if (token.startsWith("\"") && token.endsWith("\"") && token.length() >= 2) {
            // Remove outer quotes, escape inner quotes
            String inner = token.substring(1, token.length() - 1)
                    .replace("\"", "\\\"");
            return '"' + inner + '"';
        }

        // Boolean?
        if ("true".equalsIgnoreCase(token) || "false".equalsIgnoreCase(token)) {
            return token;
        }

        // Integer?
        try {
            Integer.valueOf(token);
            return token;
        } catch (NumberFormatException ignore) {
        }

        // Double?
        try {
            Double.valueOf(token);
            return token;
        } catch (NumberFormatException ignore) {
        }

        // Fallback: unquoted string
        return '"' + token + '"';
    }
}
