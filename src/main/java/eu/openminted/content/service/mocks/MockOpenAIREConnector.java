package eu.openminted.content.service.mocks;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.registry.domain.Facet;
import eu.openminted.registry.domain.Value;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by antleb on 11/16/16.
 */
@Component
public class MockOpenAIREConnector implements ContentConnector {

    @Override
    public SearchResult search(Query query) {
        Map<String, Facet> facets = new HashMap<>();

        facets.put("language", new Facet("language", "Language", Arrays.asList(new Value("english", 1200), new Value("french", 130), new Value("italian", 45))));
        facets.put("subject", new Facet("subject", "Subject", Arrays.asList(new Value("physics", 890), new Value("chemistry", 450))));
        facets.put("year", new Facet("year", "Publication Year", Arrays.asList(new Value("2010", 390), new Value("2011", 401), new Value("2012", 509))));

        return new SearchResult(new ArrayList<>(), 1690, 0, 9, facets);
    }

    @Override
    public InputStream downloadFullText(String documentId) {
        return null;
    }

    @Override
    public String getSourceName() {
        return "MockOpenAIRE";
    }
}
