package eu.openminted.content.service;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.utils.faceting.OMTDFacetEnum;
import eu.openminted.content.connector.utils.faceting.OMTDFacetLabels;
import eu.openminted.content.service.cache.CacheClient;
import eu.openminted.content.service.database.CorpusBuilderInfoDao;
import eu.openminted.content.service.messages.JMSProducer;
import eu.openminted.content.service.model.CorpusBuilderInfoModel;
import eu.openminted.content.service.process.FetchMetadataTask;
import eu.openminted.content.service.rest.ContentServiceController;
import eu.openminted.corpus.CorpusStatus;
import eu.openminted.registry.domain.Corpus;
import eu.openminted.registry.domain.DocumentTypeEnum;
import eu.openminted.registry.domain.PublicationTypeEnum;
import eu.openminted.store.restclient.StoreRESTClient;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mitre.openid.connect.client.OIDCAuthenticationFilter;
import org.mitre.openid.connect.client.OIDCAuthenticationProvider;
import org.mitre.openid.connect.model.DefaultUserInfo;
import org.mitre.openid.connect.model.OIDCAuthenticationToken;
import org.mitre.openid.connect.model.UserInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.w3c.dom.Node;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:/springrest-servlet.xml")
public class ContentServiceBuildingTest {

    private static Logger log = Logger.getLogger(ContentServiceBuildingTest.class.getName());

    @Autowired
    private List<ContentConnector> contentConnectors;

    @Autowired
    private ContentServiceController controller;

    @Autowired
    private OMTDFacetLabels omtdFacetLabels;

    @Autowired
    private OIDCAuthenticationFilter openIdConnectAuthenticationFilter;

    @Autowired
    private OIDCAuthenticationProvider  openIdConnectAuthenticationProvider;

    @Autowired
    private AuthenticationManager authenticationManager;

    @Autowired
    private Environment environment;

    @Autowired
    private StoreRESTClient storeRESTClient;

    @Autowired
    private JMSProducer producer;

    @Autowired
    private CorpusBuilderInfoDao corpusBuilderInfoDao;

    @Autowired
    private CacheClient cacheClient;

    @Autowired
    private ThreadPoolExecutor threadPoolExecutor;

    private Query query;



    @Before
    public void init() {

        UserInfo userInfo = new DefaultUserInfo();
        userInfo.setSub("0931730097797427@openminted.eu");
        userInfo.setName("Constantine Yannoussis");
        userInfo.setEmail("kgiann78@gmail.com");

        Collection<GrantedAuthority> authorities = new ArrayList<>();
        authorities.add(new SimpleGrantedAuthority("ROLE_ADMIN"));
        authorities.add(new SimpleGrantedAuthority("ROLE_USER"));

        OIDCAuthenticationToken oidcAuthenticationToken = new OIDCAuthenticationToken("0931730097797427@openminted.eu",
                environment.getProperty("oidc.issuer"),
                userInfo,
                authorities,
                null,
                environment.getProperty("oidc.secret"),
                environment.getProperty("oidc.id"));
        SecurityContextHolder.getContext().setAuthentication(oidcAuthenticationToken);

        query = new Query();
        query.setFrom(0);
        query.setTo(1);
    }

    @Test
    @Ignore
    public void testPrepare() {
        query.setParams(new HashMap<>());
        query.getParams().put(OMTDFacetEnum.SOURCE.value(), new ArrayList<>());
        query.getParams().get(OMTDFacetEnum.SOURCE.value()).add("OpenAIRE");
        query.getParams().put(OMTDFacetEnum.DOCUMENT_LANG.value(), new ArrayList<>());
        query.getParams().get(OMTDFacetEnum.DOCUMENT_LANG.value()).add("Grc");

//        Additional parameters for filtering are in comments below

        /*
            query.getParams().get(OMTDFacetEnum.SOURCE.value()).add("CORE");
            query.getParams().put(OMTDFacetEnum.PUBLICATION_YEAR.value(), new ArrayList<>());
            query.getParams().get(OMTDFacetEnum.PUBLICATION_YEAR.value()).add("2016");
            query.getParams().get(OMTDFacetEnum.DOCUMENT_LANG.value()).add("Cs");
        */

        Corpus corpus = controller.prepare(query);
        System.out.println(corpus);
    }

    @Test
//    @Ignore
    public void testBuild() throws Exception {
        query.setKeyword("mouse");

        query.setParams(new HashMap<>());
//        query.getParams().put(OMTDFacetEnum.SOURCE.value(), new ArrayList<>());
//        query.getParams().get(OMTDFacetEnum.SOURCE.value()).add("CORE");
//        query.getParams().get(OMTDFacetEnum.SOURCE.value()).add("OpenAIRE");

        query.getParams().put(OMTDFacetEnum.DOCUMENT_LANG.value(), new ArrayList<>());
        query.getParams().get(OMTDFacetEnum.DOCUMENT_LANG.value()).add("En");
        query.getParams().put(OMTDFacetEnum.PUBLICATION_YEAR.value(), new ArrayList<>());
        query.getParams().get(OMTDFacetEnum.PUBLICATION_YEAR.value()).add("2015");
        query.getParams().put(OMTDFacetEnum.DOCUMENT_TYPE.value(), new ArrayList<>());
        query.getParams().get(OMTDFacetEnum.DOCUMENT_TYPE.value()).add(omtdFacetLabels.getDocumentTypeLabelFromEnum(DocumentTypeEnum.WITH_FULL_TEXT));
        query.getParams().put(OMTDFacetEnum.PUBLICATION_TYPE.value(), new ArrayList<>());
        query.getParams().get(OMTDFacetEnum.PUBLICATION_TYPE.value()).add(omtdFacetLabels.getPublicationTypeLabelFromEnum(PublicationTypeEnum.RESEARCH_ARTICLE));

        Corpus corpus = controller.prepare(query);
        Pattern pattern = Pattern.compile(".*archiveId=(.*)$");
        Matcher matcher = pattern.matcher(corpus.getCorpusInfo().getDatasetDistributionInfo().getDistributionLocation());
        String archiveId = "";
        Collection<Future<?>> futures = new ArrayList<>();

        if(matcher.find()) {
            archiveId = matcher.group(1);
        }

        String corpusId = corpus.getMetadataHeaderInfo().getMetadataRecordIdentifier().getValue();

        if (contentConnectors != null && !archiveId.isEmpty()) {
            OIDCAuthenticationToken authentication = (OIDCAuthenticationToken) SecurityContextHolder.getContext().getAuthentication();

            for (ContentConnector connector : contentConnectors) {

                if (query.getParams().get(OMTDFacetEnum.SOURCE.value()) != null)
                    if (!query.getParams().get(OMTDFacetEnum.SOURCE.value()).contains(connector.getSourceName())) continue;

                FetchMetadataTask metadataTask = new FetchMetadataTask(storeRESTClient,
                        authentication.getUserInfo().getSub(),
                        connector,
                        query,
                        "/tmp",
                        archiveId,
                500,
                producer);

                metadataTask.setCacheClient(cacheClient);
                metadataTask.setCorpusBuilderInfoDao(corpusBuilderInfoDao);
                metadataTask.setCorpusId(corpusId);

                futures.add(threadPoolExecutor.submit(metadataTask));
            }

            for (Future<?> future : futures) {
                future.get();
            }

            CorpusBuilderInfoModel corpusBuilderInfoModel = corpusBuilderInfoDao.find(corpusId);
            storeRESTClient.finalizeArchive(archiveId);
            corpusBuilderInfoDao.update(corpusBuilderInfoModel.getId(), "status", CorpusStatus.CREATED);
        }
    }

    @Test
    @Ignore
    public void testUser() {
        ResponseEntity<Object> responseEntity = controller.user();
        System.out.println(responseEntity.getBody());
    }

    @Test
    @Ignore
    public void replaceFilename() {
        String invalidCharacters = "[#%&{}\\\\<>*?/ $!\'\":@]";
        String filename = "# pound\n" +
                "% percent\n" +
                "& ampersand\n" +
                "{ left bracket\n" +
                "} right bracket\n" +
                "\\ back slash\n" +
                "< left angle bracket\n" +
                "> right angle bracket\n" +
                "* asterisk\n" +
                "? question mark\n" +
                "/ forward slash\n" +
                "  blank spaces\n" +
                "$ dollar sign\n" +
                "! exclamation point\n" +
                "' single quotes\n" +
                "\" double quotes\n" +
                ": colon\n" +
                "@ at sign";

        System.out.println("filename before:\n" + filename);

        Pattern pattern = Pattern.compile(invalidCharacters);
        Matcher matcher = pattern.matcher(filename);
        while (matcher.find()) {
            if (matcher.group().equalsIgnoreCase(" ")) continue;
            System.out.println(matcher.start() + " " + matcher.end() + ": " + matcher.group());
        }

        filename = filename.replaceAll(invalidCharacters, ".");

        System.out.println("filename after:\n" + filename);
    }

    private void showXML(Node node) throws TransformerException, IOException {
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.transform(new DOMSource(node), new StreamResult(System.out));
    }
}