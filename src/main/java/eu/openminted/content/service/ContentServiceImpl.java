package eu.openminted.content.service;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.content.connector.faceting.OMTDFacetEnum;
import eu.openminted.content.connector.faceting.OMTDFacetInitializer;
import eu.openminted.content.service.extensions.JMSProducer;
import eu.openminted.registry.core.domain.Facet;
import eu.openminted.registry.core.domain.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class ContentServiceImpl implements ContentService {

    @org.springframework.beans.factory.annotation.Value("${content.limit:0}")
    private Integer contentLimit;

    @Autowired
    private JMSProducer producer;

    @Autowired(required = false)
    private List<ContentConnector> contentConnectors;

    private OMTDFacetInitializer OMTDFacetInitializer = new OMTDFacetInitializer();

    @Override
    public SearchResult search(Query query) {

        query.setFrom(0);
        query.setTo(1);

        if (!query.getFacets().contains(OMTDFacetEnum.PUBLICATION_TYPE.value())) query.getFacets().add(OMTDFacetEnum.PUBLICATION_TYPE.value());
        if (!query.getFacets().contains(OMTDFacetEnum.PUBLICATION_YEAR.value())) query.getFacets().add(OMTDFacetEnum.PUBLICATION_YEAR.value());
        if (!query.getFacets().contains(OMTDFacetEnum.RIGHTS.value())) query.getFacets().add(OMTDFacetEnum.RIGHTS.value());
        if (!query.getFacets().contains(OMTDFacetEnum.DOCUMENT_LANG.value())) query.getFacets().add(OMTDFacetEnum.DOCUMENT_LANG.value());

        SearchResult result = new SearchResult();
        Facet sourceFacet = new Facet();
        result.setFacets(new ArrayList<>());

        sourceFacet.setField(OMTDFacetEnum.SOURCE.value());
        sourceFacet.setValues(new ArrayList<>());

        // retrieve connectors from query
        List<String> connectors = new ArrayList<>();
        if (query.getParams() != null) {
            if (query.getParams().containsKey(OMTDFacetEnum.SOURCE.value())
                    && query.getParams().get(OMTDFacetEnum.SOURCE.value()) != null
                    && query.getParams().get(OMTDFacetEnum.SOURCE.value()).size() > 0) {
                connectors.addAll(query.getParams().get(OMTDFacetEnum.SOURCE.value()));
            }
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
            OMTDFacetEnum facetEnum = OMTDFacetEnum.fromValue(facet.getField());
            if (facetEnum != null)
            facet.setLabel(OMTDFacetInitializer.getOmtdFacetLabels().get(facetEnum));
        }
        return result;
    }

    @Override
    public ServiceStatus status() {
        ServiceStatus serviceStatus = new ServiceStatus();
        serviceStatus.setMaxFulltextDocuments(contentLimit);
        new Thread(() -> {
            producer.send("Content service status: " +
                    "maxFulltextDocuments: " + serviceStatus.getMaxFulltextDocuments());
        }).start();
        return serviceStatus;
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
