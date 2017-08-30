package eu.openminted.content.service.cache;

import eu.openminted.content.connector.ContentConnector;

import java.io.InputStream;

public interface CacheClient {
    String setDocument(ContentConnector connector, String identifier);
    InputStream getDocument(ContentConnector connector, String identifier);
    InputStream getDocument(ContentConnector connector, String identifier, String hashKey);
}
