package eu.openminted.content.service;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Facility that contains information for the Content Service
 *
 * provides a dictionary of content connectors and a value if they are online or offline
 * provides a number of the maximum number of documents that can be downloaded
 */

@Component
public class ServiceStatus {
    private int maxFulltextDocuments;

    private Map<String, Boolean> contentConnectors;

    public ServiceStatus() {
    }

    public int getMaxFulltextDocuments() {
        return maxFulltextDocuments;
    }

    public void setMaxFulltextDocuments(int maxFulltextDocuments) {
        this.maxFulltextDocuments = maxFulltextDocuments;
    }

    public Map<String, Boolean> getContentConnectors() {
        if (this.contentConnectors == null)
            this.contentConnectors = new HashMap<>();

        return contentConnectors;
    }

    public void setContentConnectors(Map<String, Boolean> contentConnectors) {
        this.contentConnectors = contentConnectors;
    }
}
