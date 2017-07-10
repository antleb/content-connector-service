package eu.openminted.content.service;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.content.service.faceting.FacetEnum;
import eu.openminted.content.service.faceting.OmtdFacetInitializer;
import eu.openminted.registry.domain.Facet;
import eu.openminted.registry.domain.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

@Component
public class ContentServiceImpl implements ContentService {

    @Autowired(required = false)
    private List<ContentConnector> contentConnectors;
    private OmtdFacetInitializer omtdFacetInitializer = new OmtdFacetInitializer();

    @Override
    public SearchResult search(Query query) {

        query.setFrom(0);
        query.setTo(1);

        if (!query.getFacets().contains(FacetEnum.PUBLICATION_TYPE.value())) query.getFacets().add(FacetEnum.PUBLICATION_TYPE.value());
        if (!query.getFacets().contains(FacetEnum.PUBLICATION_YEAR.value())) query.getFacets().add(FacetEnum.PUBLICATION_YEAR.value());
        if (!query.getFacets().contains(FacetEnum.LICENCE.value())) query.getFacets().add(FacetEnum.LICENCE.value());
        if (!query.getFacets().contains(FacetEnum.DOCUMENT_LANG.value())) query.getFacets().add(FacetEnum.DOCUMENT_LANG.value());

        SearchResult result = new SearchResult();
        Facet sourceFacet = new Facet();
        result.setFacets(new ArrayList<>());

        sourceFacet.setField(FacetEnum.SOURCE.value());
        sourceFacet.setValues(new ArrayList<>());

        // retrieve connectors from query
        List<String> connectors = new ArrayList<>();
        if (query.getParams() != null) {
            if (query.getParams().containsKey(FacetEnum.SOURCE.value())
                    && query.getParams().get(FacetEnum.SOURCE.value()) != null
                    && query.getParams().get(FacetEnum.SOURCE.value()).size() > 0) {
                connectors.addAll(query.getParams().get(FacetEnum.SOURCE.value()));
            }

            // remove field query "source" because this is a custom OMTD field
            if (query.getParams().containsKey(FacetEnum.SOURCE.value()))
                query.getParams().remove(FacetEnum.SOURCE.value());
            // also remove documentType (for the time being
            // it is always fullText and the result
            // will be the same as well)
            if (query.getParams().containsKey(FacetEnum.DOCUMENT_TYPE.value()))
                query.getParams().remove(FacetEnum.DOCUMENT_TYPE.value());
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
            facet.setLabel(omtdFacetInitializer.getOmtdFacetLabels().get(facet.getField()));
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
