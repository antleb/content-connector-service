package eu.openminted.content.service.dao;

import eu.openminted.content.service.model.CorpusBuilderInfoModel;
import eu.openminted.corpus.CorpusStatus;

import java.util.List;

/**
 * Created by constantine on 25/12/2016.
 */
public interface CorpusBuilderInfoDao {
    CorpusBuilderInfoModel find(String corpusId);

    List<CorpusBuilderInfoModel> findAll();

    void insert(String id, String query, CorpusStatus status);

    void updateStatus(String id, CorpusStatus status);
}
