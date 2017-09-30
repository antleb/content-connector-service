package eu.openminted.content.service;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.content.connector.utils.faceting.OMTDFacetEnum;
import eu.openminted.content.connector.utils.faceting.OMTDFacetLabels;
import eu.openminted.content.service.messages.JMSProducer;
import eu.openminted.registry.core.domain.Facet;
import eu.openminted.registry.core.domain.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

import static eu.openminted.content.connector.utils.SearchExtensions.merge;

@Component
public class ContentServiceImpl implements ContentService {

    @org.springframework.beans.factory.annotation.Value("${content.limit:0}")
    private Integer contentLimit;

    @Autowired
    private JMSProducer producer;

    @Autowired(required = false)
    private List<ContentConnector> contentConnectors;

    @Autowired
    private ServiceStatus serviceStatus;

    @Autowired
    private OMTDFacetLabels omtdFacetInitializer;

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

                Value sourceValue = new Value();
                sourceValue.setValue(connector.getSourceName());
                sourceValue.setCount(res.getTotalHits());
                sourceFacet.getValues().add(sourceValue);

                merge(result, res);
            }
        }

        result.getFacets().add(sourceFacet);


        // Remove values with count 0 and set labels to facets
        result.getFacets().forEach(facet-> {
            List<Value> valuesToRemove = new ArrayList<>();
            facet.getValues().forEach(value -> {

                if (value.getCount() == 0) valuesToRemove.add(value);
            });

            if (valuesToRemove.size() > 0) {
                facet.getValues().removeAll(valuesToRemove);
            }

            OMTDFacetEnum facetEnum = OMTDFacetEnum.fromValue(facet.getField());
            if (facetEnum != null)
                facet.setLabel(omtdFacetInitializer.getOmtdFacetLabels().get(facetEnum));
        });

        return result;
    }

    @Override
    public ServiceStatus status() {
        serviceStatus.setMaxFulltextDocuments(contentLimit);
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
