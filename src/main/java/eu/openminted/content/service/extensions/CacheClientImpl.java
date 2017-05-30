package eu.openminted.content.service.extensions;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.omtdcache.CacheDataID;
import eu.openminted.omtdcache.CacheDataIDMD5;
import eu.openminted.omtdcache.core.*;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

@Component
public class CacheClientImpl implements CacheClient {
    private static Logger log = Logger.getLogger(CacheClientImpl.class.toString());

    @org.springframework.beans.factory.annotation.Value("${cache.id}")
    private String cacheId;

    @org.springframework.beans.factory.annotation.Value("${store.host}")
    private String storeRestClientEndpoint;

    @org.springframework.beans.factory.annotation.Value("${cache.buckets.count}")
    private String cacheBucketCount;

    @org.springframework.beans.factory.annotation.Value("${cache.overwrite}")
    private String cacheOverwite;

    private Cache myCache;
    private CacheDataID cacheDataIDProvider;

    @PostConstruct
    public void init() {
        // Define cache properties
        CacheProperties cacheProperties = new CacheProperties();

        // properties in application.properties are strings, so they need to be converted
        Integer bucketsCount = Integer.parseInt(cacheBucketCount);
        boolean overwrite = Boolean.parseBoolean(cacheOverwite);
        cacheProperties.setBuckets(bucketsCount);
        cacheProperties.setOverwrite(overwrite);

        cacheProperties.setCacheID(cacheId);
        cacheProperties.setRestEndpoint(storeRestClientEndpoint);
        cacheProperties.setType(CacheOMTDStoreImpl.class.getName());

        myCache = CacheFactory.getCache(cacheProperties);
        cacheDataIDProvider = new CacheDataIDMD5();
    }

    public boolean setDocument(ContentConnector connector, String identifier, String hashKey) {
        try {
            InputStream inputStream = connector.downloadFullText(identifier);

            if (inputStream != null) {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                IOUtils.copy(inputStream, outputStream);

                Data data = new Data(outputStream.toByteArray());
                return myCache.putData(hashKey, data);
            }

        } catch (IOException e) {
            log.error("Error inserting document", e);
        }
        return false;
    }

    public InputStream getDocument(ContentConnector connector, String identifier, String hashkey) {
        InputStream inputStream = null;
        try {
            boolean existsInCache = myCache.contains(hashkey);
            if (!existsInCache) {
                log.debug("document not found... Inserting document now!");
                existsInCache = setDocument(connector, identifier, hashkey);
            }

            if (existsInCache) {
                log.debug("document found... Retrieving document now!");
                Data retrievedData = myCache.getData(hashkey);
                if (retrievedData != null) {
                    inputStream = new ByteArrayInputStream(retrievedData.getBytes());
                }
            }
        } catch (Exception e) {
            log.error("Error retrieving document", e);
        }
        return inputStream;
    }
}
