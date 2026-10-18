package org.operaton.bpm.extension.nifi.processor;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Conversion between {@link VariablesFormat#SIMPLE} JSON and Operaton typed variables JSON.
 *
 * <p>Simple JSON loses Operaton types (e.g. Long and Date), so the original types of fetched
 * variables are kept separately (see {@link #types}) and restored by {@link #toTyped} when
 * the simple value still fits them.
 */
final class Variables {

    // Decimals are read as BigDecimal to detect values Double cannot represent.
    static final ObjectMapper MAPPER = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .build();

    /** Types whose simple JSON value is a string of the same content. */
    private static final Set<String> TEXTUAL_TYPES = Set.of("String", "Date", "Xml", "Bytes", "Object");

    private Variables() {
    }

    /**
     * Convert typed variables JSON from the service into the given format.
     */
    static byte[] fromTyped(String typedJson, VariablesFormat format) throws IOException {
        JsonNode typed = MAPPER.readTree(typedJson == null ? "{}" : typedJson);
        if (format == VariablesFormat.TYPED) {
            return MAPPER.writeValueAsBytes(typed);
        }
        ObjectNode simple = JsonNodeFactory.instance.objectNode();
        for (Map.Entry<String, JsonNode> variable : typed.properties()) {
            simple.set(variable.getKey(), toSimple(variable.getValue()));
        }
        return MAPPER.writeValueAsBytes(simple);
    }

    /**
     * Extract the types of typed variables JSON as {@code {"name": {"type": ..., "valueInfo": ...}}}.
     */
    static String types(String typedJson) throws IOException {
        JsonNode typed = MAPPER.readTree(typedJson == null ? "{}" : typedJson);
        ObjectNode types = JsonNodeFactory.instance.objectNode();
        for (Map.Entry<String, JsonNode> variable : typed.properties()) {
            ObjectNode type = types.putObject(variable.getKey());
            type.put("type", variable.getValue().path("type").asText());
            JsonNode valueInfo = variable.getValue().get("valueInfo");
            if (valueInfo != null && valueInfo.isObject() && !valueInfo.isEmpty()) {
                type.set("valueInfo", valueInfo);
            }
        }
        return MAPPER.writeValueAsString(types);
    }

    /**
     * Convert FlowFile content in the given format into typed variables JSON for the service.
     *
     * @param typesJson original variable types from {@link #types}, or null
     * @return typed variables JSON, or null when there are no variables
     */
    static String toTyped(byte[] content, VariablesFormat format, String typesJson) throws IOException {
        if (format == VariablesFormat.NONE || content.length == 0) {
            return null;
        }
        JsonNode input = MAPPER.readTree(content);
        if (input == null || input.isMissingNode()) {
            return null;
        }
        if (!input.isObject()) {
            throw new IOException("Variables must be a JSON object, got " + input.getNodeType());
        }
        if (format == VariablesFormat.TYPED) {
            return MAPPER.writeValueAsString(input);
        }
        JsonNode types = typesJson == null || typesJson.isBlank()
                ? JsonNodeFactory.instance.objectNode() : MAPPER.readTree(typesJson);
        ObjectNode typed = JsonNodeFactory.instance.objectNode();
        for (Map.Entry<String, JsonNode> variable : input.properties()) {
            JsonNode original = types.get(variable.getKey());
            if (original != null && "File".equals(original.path("type").asText())) {
                // File contents are not fetched, so the simple value cannot be written back.
                continue;
            }
            typed.set(variable.getKey(), toTyped(variable.getKey(), variable.getValue(), original));
        }
        return MAPPER.writeValueAsString(typed);
    }

    private static JsonNode toSimple(JsonNode typed) {
        JsonNode value = typed.get("value");
        if (value == null || value.isNull()) {
            return JsonNodeFactory.instance.nullNode();
        }
        String type = typed.path("type").asText();
        if ("Json".equals(type) && value.isTextual()) {
            try {
                return MAPPER.readTree(value.asText());
            } catch (JsonProcessingException e) {
                return value;
            }
        }
        return value;
    }

    private static JsonNode toTyped(String name, JsonNode value, JsonNode original)
            throws IOException {
        String originalType = original == null ? null : original.path("type").asText();
        if (originalType != null && !value.isNull() && fits(originalType, value)) {
            ObjectNode typed = JsonNodeFactory.instance.objectNode();
            typed.put("type", originalType);
            if ("Json".equals(originalType)) {
                typed.put("value", MAPPER.writeValueAsString(value));
            } else {
                typed.set("value", value);
            }
            JsonNode valueInfo = original.get("valueInfo");
            if (valueInfo != null) {
                typed.set("valueInfo", valueInfo);
            }
            return typed;
        }
        return guessType(name, value);
    }

    /**
     * @return true when the simple value can be written back as the original type
     */
    private static boolean fits(String type, JsonNode value) {
        return switch (type) {
            case "Short" -> value.isIntegralNumber() && value.canConvertToInt()
                    && value.asInt() >= Short.MIN_VALUE && value.asInt() <= Short.MAX_VALUE;
            case "Integer" -> value.isIntegralNumber() && value.canConvertToInt();
            case "Long" -> value.isIntegralNumber() && value.canConvertToLong();
            case "Double" -> value.isNumber() && fitsDouble(value);
            case "Boolean" -> value.isBoolean();
            case "Json" -> value.isContainerNode();
            default -> TEXTUAL_TYPES.contains(type) && value.isTextual();
        };
    }

    private static boolean fitsDouble(JsonNode value) {
        if (!value.isBigDecimal()) {
            return !value.isBigInteger() || value.canConvertToLong();
        }
        BigDecimal decimal = value.decimalValue();
        double d = decimal.doubleValue();
        return Double.isFinite(d) && new BigDecimal(Double.toString(d)).compareTo(decimal) == 0;
    }

    private static JsonNode guessType(String name, JsonNode value) throws IOException {
        ObjectNode typed = JsonNodeFactory.instance.objectNode();
        if (value.isNull()) {
            typed.put("type", "Null").putNull("value");
        } else if (value.isTextual()) {
            typed.put("type", "String").set("value", value);
        } else if (value.isBoolean()) {
            typed.put("type", "Boolean").set("value", value);
        } else if (value.isIntegralNumber() && value.canConvertToInt()) {
            typed.put("type", "Integer").put("value", value.asInt());
        } else if (value.isIntegralNumber() && value.canConvertToLong()) {
            typed.put("type", "Long").put("value", value.asLong());
        } else if (value.isNumber() && !value.isIntegralNumber() && fitsDouble(value)) {
            typed.put("type", "Double").put("value", value.doubleValue());
        } else if (value.isNumber()) {
            throw new IOException("Variable " + name + " (" + value
                    + ") does not fit Long or Double; send it as a string or use typed format");
        } else {
            typed.put("type", "Json").put("value", MAPPER.writeValueAsString(value));
        }
        return typed;
    }
}
