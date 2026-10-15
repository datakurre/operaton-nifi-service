package org.operaton.bpm.extension.nifi.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;

public class VariablesTest {

    private static final String FETCHED = """
            {"long": {"type": "Long", "value": 5, "valueInfo": {}},
             "short": {"type": "Short", "value": 1, "valueInfo": {}},
             "double": {"type": "Double", "value": 1.5, "valueInfo": {}},
             "date": {"type": "Date", "value": "2024-01-01T00:00:00.000+0000", "valueInfo": {}},
             "xml": {"type": "Xml", "value": "<a/>", "valueInfo": {}},
             "object": {"type": "Object", "value": "rO0AB",
                        "valueInfo": {"objectTypeName": "com.example.Foo",
                                      "serializationDataFormat": "application/x-java-serialized-object"}},
             "json": {"type": "Json", "value": "{\\"id\\":1}", "valueInfo": {}},
             "file": {"type": "File", "value": null,
                      "valueInfo": {"filename": "a.txt", "mimeType": "text/plain"}},
             "string": {"type": "String", "value": "x", "valueInfo": {}}}
            """;

    private static JsonNode json(String content) throws IOException {
        return Variables.MAPPER.readTree(content);
    }

    private static JsonNode toTyped(String simple, String types) throws IOException {
        return json(Variables.toTyped(simple.getBytes(StandardCharsets.UTF_8), VariablesFormat.SIMPLE, types));
    }

    @Test
    public void testSimpleRoundTripKeepsTypes() throws IOException {
        byte[] simple = Variables.fromTyped(FETCHED, VariablesFormat.SIMPLE);
        String types = Variables.types(FETCHED);

        JsonNode typed = json(Variables.toTyped(simple, VariablesFormat.SIMPLE, types));

        JsonNode expected = json(FETCHED);
        ((ObjectNode) expected).remove("file");
        for (String name : new String[] {"long", "short", "double", "date", "xml", "object", "string"}) {
            assertEquals(expected.at("/" + name + "/type"), typed.at("/" + name + "/type"), name);
            assertEquals(expected.at("/" + name + "/value"), typed.at("/" + name + "/value"), name);
        }
        assertEquals("com.example.Foo", typed.at("/object/valueInfo/objectTypeName").asText());
        assertEquals("Json", typed.at("/json/type").asText());
        assertEquals(json("{\"id\":1}"), json(typed.at("/json/value").asText()));
        assertEquals(expected.size(), typed.size(), "File variables are not written back");
    }

    @Test
    public void testChangedValueOutsideOriginalTypeIsGuessed() throws IOException {
        String types = Variables.types(FETCHED);

        JsonNode typed = toTyped("{\"short\": 100000, \"date\": 1, \"new\": 2}", types);

        assertEquals("Integer", typed.at("/short/type").asText());
        assertEquals("Integer", typed.at("/date/type").asText());
        assertEquals("Integer", typed.at("/new/type").asText());
    }

    @Test
    public void testNullValueClearsVariable() throws IOException {
        JsonNode typed = toTyped("{\"long\": null}", Variables.types(FETCHED));

        assertEquals("Null", typed.at("/long/type").asText());
    }

    @Test
    public void testGuessedNumberTypes() throws IOException {
        JsonNode typed = toTyped("{\"i\": 1, \"l\": 12345678901, \"d\": 0.1, \"e\": 1e3}", null);

        assertEquals("Integer", typed.at("/i/type").asText());
        assertEquals("Long", typed.at("/l/type").asText());
        assertEquals("Double", typed.at("/d/type").asText());
        assertEquals(0.1, typed.at("/d/value").asDouble());
        assertEquals("Double", typed.at("/e/type").asText());
    }

    @Test
    public void testPreciseDecimalIsRejected() {
        IOException e = assertThrows(IOException.class,
                () -> toTyped("{\"amount\": 0.1000000000000000055}", null));
        assertEquals("Variable amount (0.1000000000000000055) does not fit Long or Double; "
                + "send it as a string or use typed format", e.getMessage());
    }

    @Test
    public void testHugeIntegerIsRejected() {
        assertThrows(IOException.class, () -> toTyped("{\"n\": 123456789012345678901234567890}", null));
    }

    @Test
    public void testNonObjectIsRejected() {
        assertThrows(IOException.class, () -> toTyped("[1]", null));
    }
}
