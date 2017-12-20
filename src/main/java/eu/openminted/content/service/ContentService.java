package eu.openminted.content.service;

import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;

import java.io.IOException;

public interface ContentService {

    /**
     * Standard method for returning documents from service
     * @param query
     * @return SearchResult
     */
    SearchResult search(Query query) throws IOException;

    /**
     * Standard method that returns the status of the current service
     * @return ServiceStatus
     */
    ServiceStatus status();
}
