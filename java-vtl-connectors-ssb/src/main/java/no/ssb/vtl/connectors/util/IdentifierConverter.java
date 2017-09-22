package no.ssb.vtl.connectors.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;

public class IdentifierConverter {
    
    public static String toQueryString(String json) throws IOException {
        StringBuilder sb = new StringBuilder();
        JsonNode jsonNode = new ObjectMapper().readTree(json);
        JsonNode query = jsonNode.get("query");
        if (query.size() > 0) {
            for (int q = 0; q < query.size(); q++) {
                if (q > 0) {
                    sb.append("&");
                }
                JsonNode queryPart = query.get(q);
                sb.append(trimQuotes(queryPart.get("code")));
                
                sb.append("=");
                JsonNode values = queryPart.get("selection").get("values"); //TODO: Support other filter types
                for (int v = 0; v < values.size(); v++) {
                    if (v > 0) {
                        sb.append("+");
                    }
                    JsonNode value = values.get(v);
                    sb.append(trimQuotes(value));
                    
                }
            }
        }
        return sb.toString();
        
        
    }
    
    
    
    public static JsonNode toJson(String query) {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode json = factory.objectNode();
        ArrayNode array = json.putArray("query");
        String[] parameters = query.split("&");
        for (String parameter : parameters) {
            ObjectNode queryPart = array.addObject();
            String[] keyValue = parameter.split("=");
            String key = keyValue[0];
            String value = keyValue[1];
            queryPart.put("code", key);
            ObjectNode selection = queryPart.putObject("selection");
            selection.put("filter", "item"); //TODO: Support other filter types
            ArrayNode valuesNode = selection.putArray("values");
            String[] values = value.split("\\+");
            for (String value1 : values) {
                valuesNode.add(value1);
            }
            
            
        }
        ObjectNode response = json.putObject("response");
        response.put("format", "json-stat");
        return json;
    }
    
    private static String trimQuotes(JsonNode node) {
        return node.toString().replaceAll("\"", "");
    }
    
}























