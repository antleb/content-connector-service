package eu.openminted.content.service.threads;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.registry.core.domain.Facet;
import eu.openminted.registry.core.domain.Value;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.Callable;


public class QueryTask implements Callable<QueryResult> {

    private ContentConnector contentConnector;

    private Query query;

    private List<String> connectors;


    private static Logger logger = Logger.getLogger(ThreadpoolWrapper.class);

    public QueryTask(ContentConnector contentConnector, Query query, List<String> connectors) {
        this.contentConnector = contentConnector;   // Connector to apply query
        this.query = query; // The query
        this.connectors = connectors;   // List of connectors: contentConnector should be included
    }

    @Override
    public QueryResult call() {

        if (connectors.size() > 0 && !connectors.contains(contentConnector.getSourceName())) {
            return null;
        }

        SearchResult res;
        try {
            res = contentConnector.search(query); // TODO query
            Value sourceValue = new Value();
            sourceValue.setValue(contentConnector.getSourceName());
            sourceValue.setLabel(contentConnector.getSourceName());
            sourceValue.setCount(res.getTotalHits());
            return new QueryResult(res, sourceValue);
            // TODO ektos apo ta apotelesmata to query, allazei kai to periexomeno tis sourceFacet kai prepei na epistrafei sto main thread: QueryResult class
        } catch (Exception e) {
            logger.error("Error thread searching in " + contentConnector.getSourceName(), e);
            for (String facet : query.getFacets()) {
                logger.error(facet);
            }
            return null;
        }
    }
}