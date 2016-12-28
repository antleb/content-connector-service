package eu.openminted.content.service.dao;

import eu.openminted.content.service.model.CorpusModel;

import java.util.List;

/**
 * Created by constantine on 25/12/2016.
 */
public interface CorpusDao {
    CorpusModel find(String corpusId);

    List<CorpusModel> findAll();

    void insert(String corpusId, String query);
}
