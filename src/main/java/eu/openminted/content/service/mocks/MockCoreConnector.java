package eu.openminted.content.service.mocks;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.registry.domain.Facet;
import eu.openminted.registry.domain.Value;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.*;

/**
 * Created by antleb on 11/16/16.
 */
@Component
public class MockCoreConnector implements ContentConnector {
    @Override
    public SearchResult search(Query query) {
        List<Facet> facets = new ArrayList<>();

        facets.add(new Facet("language", "Language", Arrays.asList(new Value("english", 1200), new Value("french", 130), new Value("italian", 45), new Value("greel", 1011))));
        facets.add(new Facet("subject", "Subject", Arrays.asList(new Value("physics", 1890), new Value("maths", 230))));
        facets.add(new Facet("licence", "Licence", Arrays.asList(new Value("CC", 890), new Value("CC-BY", 980), new Value("CC-A-B-C", 1032))));

        return new SearchResult(new ArrayList<>(), 2690, 0, 9, facets);
    }

    @Override
    public InputStream downloadFullText(String documentId) {
        return null;
    }

    @Override
    public InputStream fetchMetadata(Query query) {
        return null;
    }

    @Override
    public String getSourceName() {
        return "MockCORE";
    }
}