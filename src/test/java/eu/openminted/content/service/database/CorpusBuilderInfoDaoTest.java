package eu.openminted.content.service.database;

import eu.openminted.content.service.ServiceConfiguration;
import eu.openminted.content.service.model.CorpusBuilderInfoModel;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ServiceConfiguration.class})
public class CorpusBuilderInfoDaoTest {
    @Autowired
    CorpusBuilderInfoDao corpusBuilderInfoDao;

    @Autowired
    NamedParameterJdbcTemplate jdbcTemplate;

    @Test
    @Ignore
    public void findAll() {
        List<CorpusBuilderInfoModel> results = corpusBuilderInfoDao.findAll();

        System.out.println("\nFIND ALL CORPUS BUILDING REQUESTS");
        for (CorpusBuilderInfoModel model : results) {
            System.out.println(model);
        }

        System.out.println("\nFIND ALL UNFINISHED CORPUS BUILDING REQUESTS:");

        results = corpusBuilderInfoDao.findAllUnfinished();

        for (CorpusBuilderInfoModel model : results) {
            System.out.println(model);
        }
    }
}
