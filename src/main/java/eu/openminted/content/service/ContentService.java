package eu.openminted.content.service;

import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;

/**
 * Created by antleb on 11/16/16.
 */
public interface ContentService {

    public SearchResult search(Query query);
}
