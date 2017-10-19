package eu.openminted.content.service.model;

import javax.persistence.Entity;

@Entity
public class CorpusBuilderInfoModel {
    private String id;
    private String token;
    private String query;
    private String corpus;
    private String status;
    private String archiveId;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getCorpus() {
        return corpus;
    }

    public void setCorpus(String corpus) {
        this.corpus = corpus;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getArchiveId() {
        return archiveId;
    }

    public void setArchiveId(String archiveId) {
        this.archiveId = archiveId;
    }

    @Override
    public String toString() {
        return "CorpusBuilderInfoModel{" +
                "id='" + id + '\'' +
                ", token='" + token + '\'' +
                ", query='" + query + '\'' +
                ", corpus='" + corpus + '\'' +
                ", status='" + status + '\'' +
                ", archiveId='" + archiveId + '\'' +
                '}';
    }
}
