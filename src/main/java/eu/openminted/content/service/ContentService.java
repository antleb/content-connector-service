package eu.openminted.content.service;

import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;

public interface ContentService {

    /**
     * Standard method for returning documents from service
     * @param query
     * @return SearchResult
     */
    SearchResult search(Query query);

    /**
     * Standard method that returns the status of the current service
     * @return ServiceStatus
     */
    ServiceStatus status();
}
