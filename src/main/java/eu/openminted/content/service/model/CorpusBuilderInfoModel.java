package eu.openminted.content.service.model;

import javax.persistence.Entity;

@Entity
public class CorpusBuilderInfoModel {
    private String id;
    private String query;
    private String status;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "CorpusBuilderInfoModel{" +
                "id='" + id + '\'' +
                ", query='" + query + '\'' +
                ", status='" + status + '\'' +
                '}';
    }
}
