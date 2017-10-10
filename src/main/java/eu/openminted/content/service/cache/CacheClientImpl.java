package eu.openminted.content.service.cache;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.omtdcache.CacheDataID;
import eu.openminted.omtdcache.CacheDataIDSHA1;
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
    private Integer cacheBucketCount;

    @org.springframework.beans.factory.annotation.Value("${cache.overwrite}")
    private Boolean cacheOverwite;

    private Cache myCache;
    private CacheDataID cacheDataIDProvider;

    @PostConstruct
    public void init() {
        // Define cache properties
        CacheProperties cacheProperties = new CacheProperties();
        cacheProperties.setBuckets(cacheBucketCount);
        cacheProperties.setOverwrite(cacheOverwite);
        cacheProperties.setCacheID(cacheId);
        cacheProperties.setRestEndpoint(storeRestClientEndpoint);
        cacheProperties.setType(CacheOMTDStoreImpl.class.getName());

        myCache = CacheFactory.getCache(cacheProperties);
        cacheDataIDProvider = new CacheDataIDSHA1();
    }

    public String setDocument(ContentConnector connector, String identifier) {
        try {
            InputStream inputStream = connector.downloadFullText(identifier);

            if (inputStream != null) {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                IOUtils.copy(inputStream, outputStream);

                Data data = new Data(outputStream.toByteArray());

                String hashKey = cacheDataIDProvider.getID(data.getBytes());
                boolean isInserted = myCache.putData(hashKey, data);
                if (isInserted)
                    return hashKey;
                else return "";
            }

        } catch (IOException e) {
            log.error("Error inserting document", e);
        }
        return "";
    }

    public InputStream getDocument(ContentConnector connector, String identifier) {
        InputStream inputStream = null;

        String hashΚey = setDocument(connector, identifier);

        if (!hashΚey.isEmpty()) {
            try {
                Data retrievedData = myCache.getData(hashΚey);
                if (retrievedData != null) {
                    inputStream = new ByteArrayInputStream(retrievedData.getBytes());
                }

            } catch (Exception e) {
                log.error("Error retrieving document", e);
            }
        }

        return inputStream;
    }

    public InputStream getDocument(ContentConnector connector, String identifier, String hashΚey) {
        InputStream inputStream = null;
        try {
            boolean existsInCache = myCache.contains(hashΚey);

            if (!existsInCache) {
                log.debug("Document not found... Looking for document in connector!");
                hashΚey = setDocument(connector, identifier);
            }

            if (existsInCache || !hashΚey.isEmpty()) {
                log.debug("Document found... Retrieving document!");
                Data retrievedData = myCache.getData(hashΚey);
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
