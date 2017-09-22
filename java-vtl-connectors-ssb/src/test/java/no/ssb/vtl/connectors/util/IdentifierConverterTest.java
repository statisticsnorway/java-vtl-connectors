package no.ssb.vtl.connectors.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.common.io.Resources;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

public class IdentifierConverterTest {
    
    @Test
    public void testConvertJson() throws Exception {
        InputStream queryStream = Resources.getResource(this.getClass(), "/query.json").openStream();
        String query = CharStreams.toString(new InputStreamReader(queryStream, Charsets.UTF_8));
        String urlParams = IdentifierConverter.toQueryString(query);
        assertThat(urlParams).isEqualTo("Region=0+01+0101&Eierskap=01+02-03+98&ContentsCode=Antall1&Tid=1987+1988+1989");
    }
    
    @Test
    public void toJson() throws Exception {
        JsonNode jsonNode = IdentifierConverter.toJson("Region=0+01+0101&Eierskap=01+02-03+98&ContentsCode=Antall1&Tid=1987+1988+1989");
        System.out.println(jsonNode);
        assertThat(jsonNode).isNotNull();
    
        InputStream queryStream = Resources.getResource(this.getClass(), "/query.json").openStream();
        String query = CharStreams.toString(new InputStreamReader(queryStream, Charsets.UTF_8));
        JsonNode jsonNode1 = new ObjectMapper().readTree(query);
        assertThat(jsonNode).isEqualTo(jsonNode1);
    }
}