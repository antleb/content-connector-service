package eu.openminted.content.service;

public class ServiceStatus {
    private int maxFulltextDocuments;

    public ServiceStatus() {
    }

    public int getMaxFulltextDocuments() {
        return maxFulltextDocuments;
    }

    public void setMaxFulltextDocuments(int maxFulltextDocuments) {
        this.maxFulltextDocuments = maxFulltextDocuments;
    }
}
