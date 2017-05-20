package eu.openminted.content.service;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.registry.domain.Facet;
import eu.openminted.registry.domain.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Component
public class ContentServiceImpl implements ContentService {

    @Autowired(required = false)
    private List<ContentConnector> contentConnectors;

    @Override
    public SearchResult search(Query query) {

        if (query == null) {
            query = new Query("*:*", new HashMap<>(), new ArrayList<>(), 0, 1);
        } else if (query.getKeyword() == null || query.getKeyword().isEmpty()) {
            query.setKeyword("*:*");
        }

        query.setFrom(0);
        query.setTo(1);

        if (query.getFacets() == null) query.setFacets(new ArrayList<>());

        if (!containsIgnoreCase(query.getFacets(), "publicationType")) query.getFacets().add("publicationType");
        if (!containsIgnoreCase(query.getFacets(), "publicationYear")) query.getFacets().add("publicationYear");
//        if (!containsIgnoreCase(query.getFacets(), "rightsStmtName")) query.getFacets().add("rightsStmtName");
        if (!containsIgnoreCase(query.getFacets(), "Licence")) query.getFacets().add("Licence");
        if (!containsIgnoreCase(query.getFacets(), "documentLanguage")) query.getFacets().add("documentLanguage");

        SearchResult result = new SearchResult();
        Facet sourceFacet = new Facet();
        result.setFacets(new ArrayList<>());

        sourceFacet.setField("source");
        sourceFacet.setLabel("Content Source");
        sourceFacet.setValues(new ArrayList<>());

        if (contentConnectors != null) {
            for (ContentConnector connector : contentConnectors) {
                SearchResult res = connector.search(query);
                Value value = new Value();

                value.setValue(connector.getSourceName());
                value.setCount(res.getTotalHits());

                sourceFacet.getValues().add(value);
                result.merge(res).setFrom(res.getFrom()).setTo(res.getTo());
            }
        }

        result.getFacets().add(sourceFacet);

        return result;
    }

    private boolean containsIgnoreCase(List<String> list, String keyword) {

        for (String item : list) {
            if (item.equalsIgnoreCase(keyword)) return true;
        }
        return false;
    }

    public List<ContentConnector> getContentConnectors() {
        return contentConnectors;
    }

    public void setContentConnectors(List<ContentConnector> contentConnectors) {
        this.contentConnectors = contentConnectors;
    }
}
