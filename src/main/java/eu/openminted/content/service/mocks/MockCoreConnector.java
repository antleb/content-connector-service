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

        facets.add(new Facet("documentLanguage", "documentLanguage", Arrays.asList(new Value("English", 1200), new Value("French", 130), new Value("Italian", 45), new Value("Greek", 1011))));
        facets.add(new Facet("licence", "licence", Arrays.asList(new Value("Closed Access", 1890), new Value("Open Access", 230))));
        facets.add(new Facet("publicationDate", "publicationDate", Arrays.asList(new Value("2010", 890), new Value("2011", 980), new Value("2012", 1032))));
        facets.add(new Facet("documentType", "documentType", Arrays.asList(new Value("fullText", 2690))));

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