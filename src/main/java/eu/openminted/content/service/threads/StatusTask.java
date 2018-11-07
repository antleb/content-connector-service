package eu.openminted.content.service.threads;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;

/**
 * Callable implementation. Contains the code to be executed by every thread:
 *      Query the ContentConnector to determine its status (true(available)/false(unavailable))
 */

public class StatusTask implements Callable<String> {

    private ContentConnector contentConnector;
    private static Logger logger = Logger.getLogger(ThreadpoolWrapper.class);

    public StatusTask(ContentConnector contentConnector) {
        this.contentConnector = contentConnector;
    }

    public ContentConnector getContentConnector() {
        return contentConnector;
    }

    public void setContentConnector(ContentConnector contentConnector) {
        this.contentConnector = contentConnector;
    }

    @Override
    public String call() {

        SearchResult searchResult;
        try {
            logger.debug("Querying content connector");
            searchResult = contentConnector.search(new Query("*", new HashMap<>(), new ArrayList<>(), 0, 1));
        } catch (IOException e) {
            searchResult = null;
            logger.error("Content connector query IOException");
        }
        if (searchResult != null && searchResult.getTotalHits() > 0) {
            return contentConnector.getSourceName();
        } else {
            return null;
        }
    }

}
