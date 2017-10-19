package eu.openminted.content.service;

import eu.openminted.store.restclient.StoreRESTClient;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
public class ConnectorServiceTestConfiguration {
    @Value("${jms.host}")
    private String jmsHost;

    @Value("${store.host}")
    private String storeHost;

    @Bean
    public static PropertyPlaceholderConfigurer propertyPlaceholderConfigurer() throws IOException {
        final PropertyPlaceholderConfigurer ppc = new PropertyPlaceholderConfigurer();
        ppc.setLocations(new ClassPathResource("application.properties"), new ClassPathResource("tokens-override.properties"));
        ppc.setIgnoreUnresolvablePlaceholders(true);
        ppc.setIgnoreResourceNotFound(true);
        return ppc;
    }

    @Bean
    public ActiveMQConnectionFactory connectionFactory() {
        System.out.println(jmsHost);
        return new ActiveMQConnectionFactory();
    }

    @Bean
    public LinkedBlockingQueue workQueue() {
        return new LinkedBlockingQueue();
    }

    @Bean
    public ThreadPoolExecutor threadPoolExecutor() {
        return new ThreadPoolExecutor(10, 10, 0, TimeUnit.MILLISECONDS, workQueue());
    }

    @Bean
    public StoreRESTClient storeRESTClient() {
        return new StoreRESTClient(storeHost);
    }

    @Bean
    public DataSource dbcpDataSource() {

//        EmbeddedDatabaseBuilder databaseBuilder = new EmbeddedDatabaseBuilder();
//        EmbeddedDatabase db = databaseBuilder
//                .setType(EmbeddedDatabaseType.HSQL) //.H2 or .DERBY
//                .addScript("db/sql/create-db.sql")
//                .addScript("db/sql/insert-data.sql")
//                .build();
//        return db;

//        BasicDataSource basicDataSource = new BasicDataSource();
//        basicDataSource.setDriverClassName("org.hsqldb.jdbcDriver");
//        basicDataSource.setUrl("jdbc:hsqldb:mem:corpusDb");
//        basicDataSource.setUsername("sa");
//        basicDataSource.setPassword("");
//        return basicDataSource;
//
        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setDriverClassName("org.postgresql.Driver");
        basicDataSource.setUrl("jdbc:postgresql://83.212.101.85:5432/corpusbuilder");
        basicDataSource.setUsername("vrasidas");
        basicDataSource.setPassword("paparia");
        return basicDataSource;
    }

    @Bean
    public NamedParameterJdbcTemplate jdbcTemplate() {
        return new NamedParameterJdbcTemplate(dbcpDataSource());
    }
}
