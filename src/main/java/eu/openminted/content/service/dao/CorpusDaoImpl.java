package eu.openminted.content.service.dao;

import eu.openminted.content.service.model.CorpusModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by constantine on 25/12/2016.
 */
@Repository
public class CorpusDaoImpl implements CorpusDao{

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    public void setNamedParameterJdbcTemplate(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public CorpusModel find(String id) {

        Map<String, Object> params = new HashMap<>();
        params.put("id", id);
        String sql = "SELECT * FROM corpus WHERE id=:id;";

        return this.jdbcTemplate.queryForObject(sql, params, new BeanPropertyRowMapper<>(CorpusModel.class));
    }

    @Override
    public List<CorpusModel> findAll() {
        String sql = "SELECT * FROM corpus;";
        return this.jdbcTemplate.query(sql, new BeanPropertyRowMapper<>(CorpusModel.class));
    }

    @Override
    public void insert(String id, String query) {
        Map<String, Object> params = new HashMap<>();
        params.put("id", id);
        params.put("query", query);

        this.jdbcTemplate.update("insert into corpus values (:id, :query);", params);
    }
}
