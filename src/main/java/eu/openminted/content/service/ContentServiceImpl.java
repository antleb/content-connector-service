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

        // retrieve connectors from query
        List<String> connectors = new ArrayList<>();
        if (query.getParams().containsKey("source")
                && query.getParams().get("source") != null
                && query.getParams().get("source").size() > 0) {
            connectors.addAll(query.getParams().get("source"));
        }

        // remove field query "source" because this is a custom OMTD field
        if (query.getParams().containsKey("source"))
            query.getParams().remove("source");
        // also remove documentType (for the time being
        // it is always fullText and the result
        // will be the same as well)
        if (query.getParams().containsKey("documentType"))
            query.getParams().remove("documentType");


        if (contentConnectors != null) {
            for (ContentConnector connector : contentConnectors) {

                if (connectors.size() > 0 && !connectors.contains(connector.getSourceName())) continue;

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
