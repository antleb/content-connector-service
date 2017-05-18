package eu.openminted.content.service.extensions;

import eu.openminted.content.connector.ContentConnector;

import java.io.InputStream;

public interface CacheClient {
    boolean setDocument(ContentConnector connector, String identifier);
    InputStream getDocument(ContentConnector connector, String identifier);
}
