package eu.openminted.content.service;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.content.connector.utils.faceting.OMTDFacetEnum;
import eu.openminted.content.connector.utils.faceting.OMTDFacetLabels;
import eu.openminted.content.service.messages.JMSProducer;
import eu.openminted.content.service.threads.StatusTask;
import eu.openminted.content.service.threads.ThreadpoolWrapper;
import eu.openminted.registry.core.domain.Facet;
import eu.openminted.registry.core.domain.Value;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;

import static eu.openminted.content.connector.utils.SearchExtensions.merge;

@Component
public class ContentServiceImpl implements ContentService {

    private static Logger log = Logger.getLogger(ContentConnector.class);

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
    public SearchResult search(Query query) throws IOException {

        query.setFrom(0);
        query.setTo(1);

        if (!query.getFacets().contains(OMTDFacetEnum.PUBLICATION_TYPE.value())) query.getFacets().add(OMTDFacetEnum.PUBLICATION_TYPE.value());
        if (!query.getFacets().contains(OMTDFacetEnum.PUBLICATION_YEAR.value())) query.getFacets().add(OMTDFacetEnum.PUBLICATION_YEAR.value());
        if (!query.getFacets().contains(OMTDFacetEnum.RIGHTS.value())) query.getFacets().add(OMTDFacetEnum.RIGHTS.value());
        if (!query.getFacets().contains(OMTDFacetEnum.DOCUMENT_LANG.value())) query.getFacets().add(OMTDFacetEnum.DOCUMENT_LANG.value());
        if (!query.getFacets().contains(OMTDFacetEnum.DOCUMENT_TYPE.value())) query.getFacets().add(OMTDFacetEnum.DOCUMENT_TYPE.value());

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

                try {

                    SearchResult res = connector.search(query);

                    Value sourceValue = new Value();
                    sourceValue.setValue(connector.getSourceName());
                    sourceValue.setLabel(connector.getSourceName());
                    sourceValue.setCount(res.getTotalHits());
                    sourceFacet.getValues().add(sourceValue);

                    merge(result, res);
                } catch (Exception e){
                    log.error("Error searching in " + connector.getSourceName(), e);
                    for(String facet : query.getFacets())
                        log.error(facet);
                }
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
                facet.setLabel(omtdFacetInitializer.getFacetLabelsFromEnum(facetEnum));
        });

        return result;
    }

    /**
     *  Standard method that returns the status of the current service
     *
     * @return ServiceStatus object
     */
//    @Override
//    public ServiceStatus status() {
//        serviceStatus.setMaxFulltextDocuments(contentLimit);
//
//        for (ContentConnector connector : contentConnectors) {
//
//            try {
//                SearchResult searchResult;
//                try {
//                    searchResult = connector.search(new Query("*", new HashMap<>(), new ArrayList<>(), 0, 1));
//                } catch (IOException e ) {
//                    searchResult = null;
//                }
//
//                if (searchResult != null && searchResult.getTotalHits() > 0) {
//                    serviceStatus.getContentConnectors().put(connector.getSourceName(), true);
//                    System.out.println(connector.getSourceName() + ": " + searchResult.getTotalHits());
//                } else
//                    serviceStatus.getContentConnectors().put(connector.getSourceName(), false);
//
//            } catch (Exception e) {
//                log.error("Error retrieving content connector");
//            }
//        }
//        return serviceStatus;
//    }

    public ServiceStatus status() {

        serviceStatus.setMaxFulltextDocuments(contentLimit);
        ThreadpoolWrapper threadpool = new ThreadpoolWrapper(Executors.newFixedThreadPool(2));

        // Create and add tasks to the queue
        for (ContentConnector connector : contentConnectors) {
            threadpool.addTask(new StatusTask(connector));
        }

        threadpool.invokeAll(4);
        threadpool.shutdown();

        List<String> results = threadpool.getResults();
        for (ContentConnector connector : contentConnectors) {
            if (results.contains(connector.getSourceName())) {
                serviceStatus.getContentConnectors().put(connector.getSourceName(), true);
            } else {
                serviceStatus.getContentConnectors().put(connector.getSourceName(), false);
            }
        }
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
