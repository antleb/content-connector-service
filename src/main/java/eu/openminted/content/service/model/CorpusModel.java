package eu.openminted.content.service.model;

import javax.persistence.Entity;

/**
 * Created by constantine on 25/12/2016.
 */
@Entity
public class CorpusModel {
    private String id;
    private String query;

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

    @Override
    public String toString() {
        return "CorpusModel{" +
                "id=" + id +
                ", query='" + query + '\'' +
                '}';
    }
}
