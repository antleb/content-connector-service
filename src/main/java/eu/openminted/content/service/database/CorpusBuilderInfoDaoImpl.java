package eu.openminted.content.service.database;

import eu.openminted.content.service.model.CorpusBuilderInfoModel;
import eu.openminted.corpus.CorpusStatus;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class CorpusBuilderInfoDaoImpl implements CorpusBuilderInfoDao {
    private static Logger logger = Logger.getLogger(CorpusBuilderInfoDaoImpl.class);

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    public void setNamedParameterJdbcTemplate(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public CorpusBuilderInfoModel find(String id) {

        Map<String, Object> params = new HashMap<>();
        params.put("id", id);
        String sql = "SELECT * FROM corpusbuilderinfo WHERE id=:id;";

        return this.jdbcTemplate.queryForObject(sql, params, new BeanPropertyRowMapper<>(CorpusBuilderInfoModel.class));
    }

    @Override
    public List<CorpusBuilderInfoModel> findAll() {
        String sql = "SELECT * FROM corpusbuilderinfo;";
        return this.jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(CorpusBuilderInfoModel.class));
    }

    public List<CorpusBuilderInfoModel> findAllUnfinished() {
        String sql = "SELECT * FROM corpusbuilderinfo WHERE corpusbuilderinfo.status = 'SUBMITTED' or corpusbuilderinfo.status = 'INITIATING' or  corpusbuilderinfo.status like 'PROCESSING%' ;";
        return this.jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(CorpusBuilderInfoModel.class));
    }

    @Override
    public void insert(String id, String token, String query, String corpusMetadata, CorpusStatus status, String archiveId) {
        Map<String, Object> params = new HashMap<>();
        params.put("id", id);
        params.put("token", token);
        params.put("query", query);
        params.put("corpus", corpusMetadata);
        params.put("status", status.toString());
        params.put("archiveId", archiveId);

        logger.debug("Inserting new with archiveId " + archiveId);

        this.jdbcTemplate.update("insert into corpusbuilderinfo (id,token,query,corpus,status,archiveId) values (:id, :token, :query, :corpus, :status, :archiveId);", params);
    }

    @Override
    public void updateStatus(String id, CorpusStatus status) {
        Map<String, Object> params = new HashMap<>();
        params.put("id", id);
        params.put("status", status.toString());
        this.jdbcTemplate.update("update corpusbuilderinfo set status = :status where id = :id;", params);
    }

    @Override
    public void update(String id, String field, Object value) {

        logger.debug("Updating " + field + " to '" + value + "'");

        Map<String, Object> params = new HashMap<>();
        params.put("id", id);
        params.put(field, value.toString());
        String query = String.format("update corpusbuilderinfo set %s = :%s where id = :id;", field, field);
        this.jdbcTemplate.update(query, params);
    }
}
