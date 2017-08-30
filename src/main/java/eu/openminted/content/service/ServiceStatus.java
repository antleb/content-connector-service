package eu.openminted.content.service;

import eu.openminted.content.connector.ContentConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ServiceStatus {
    private int maxFulltextDocuments;

    @Autowired(required = false)
    private List<ContentConnector> contentConnectors;

    public ServiceStatus() {
    }

    public int getMaxFulltextDocuments() {
        if (contentConnectors != null) {
            return contentConnectors.size() * maxFulltextDocuments;
        }
        return maxFulltextDocuments;
    }

    public void setMaxFulltextDocuments(int maxFulltextDocuments) {
        this.maxFulltextDocuments = maxFulltextDocuments;
    }
}
