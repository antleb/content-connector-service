package eu.openminted.content.service;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.registry.domain.Facet;
import eu.openminted.registry.domain.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class ContentServiceImpl implements ContentService {

    @Autowired(required = false)
    private List<ContentConnector> contentConnectors;

    private Map<String, String> OmtdFacetLabels = new HashMap<>();
    String PUBLICATION_TYPE = "publicationType";
    String PUBLICATION_YEAR = "publicationYear";
    String PUBLISHER = "publisher";
    String RIGHTS_STMT_NAME = "rightsStmtName";
    String LICENCE = "licence";
    String DOCUMENT_LANG = "documentLanguage";
    String DOCUMENT_TYPE = "documentType";
    String SOURCE = "source";

    @PostConstruct
    private void init() {
        OmtdFacetLabels.put(PUBLICATION_TYPE,"Publication Type");
        OmtdFacetLabels.put(PUBLICATION_YEAR,"Publication Year");
        OmtdFacetLabels.put(RIGHTS_STMT_NAME,"Rights Statement");
        OmtdFacetLabels.put(LICENCE,"Licence");
        OmtdFacetLabels.put(DOCUMENT_LANG,"Language");
        OmtdFacetLabels.put(DOCUMENT_TYPE,"Document Type");
        OmtdFacetLabels.put(SOURCE,"Content Source");
    }

    @Override
    public SearchResult search(Query query) {

        query.setFrom(0);
        query.setTo(1);

        if (!query.getFacets().contains(PUBLICATION_TYPE)) query.getFacets().add(PUBLICATION_TYPE);
        if (!query.getFacets().contains(PUBLICATION_YEAR)) query.getFacets().add(PUBLICATION_YEAR);
        if (!query.getFacets().contains(LICENCE)) query.getFacets().add(LICENCE);
        if (!query.getFacets().contains(DOCUMENT_LANG)) query.getFacets().add(DOCUMENT_LANG);

        SearchResult result = new SearchResult();
        Facet sourceFacet = new Facet();
        result.setFacets(new ArrayList<>());

        sourceFacet.setField(SOURCE);
        sourceFacet.setValues(new ArrayList<>());

        // retrieve connectors from query
        List<String> connectors = new ArrayList<>();
        if (query.getParams() != null) {
            if (query.getParams().containsKey(SOURCE)
                    && query.getParams().get(SOURCE) != null
                    && query.getParams().get(SOURCE).size() > 0) {
                connectors.addAll(query.getParams().get(SOURCE));
            }

            // remove field query "source" because this is a custom OMTD field
            if (query.getParams().containsKey(SOURCE))
                query.getParams().remove(SOURCE);
            // also remove documentType (for the time being
            // it is always fullText and the result
            // will be the same as well)
            if (query.getParams().containsKey(DOCUMENT_TYPE))
                query.getParams().remove(DOCUMENT_TYPE);
        }

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

        for (Facet facet : result.getFacets()) {
            facet.setLabel(OmtdFacetLabels.get(facet.getField()));
        }
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
