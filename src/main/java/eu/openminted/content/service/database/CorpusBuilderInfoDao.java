package eu.openminted.content.service.database;

import eu.openminted.content.service.model.CorpusBuilderInfoModel;
import eu.openminted.corpus.CorpusStatus;

import java.util.List;

public interface CorpusBuilderInfoDao {
    CorpusBuilderInfoModel find(String corpusId);

    List<CorpusBuilderInfoModel> findAll();

    void insert(String id, String query, CorpusStatus status, String archiveId);

    void updateStatus(String id, CorpusStatus status);

    void update(String id, String field, Object value);
}
