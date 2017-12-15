package eu.openminted.content.service.process;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.content.connector.utils.faceting.OMTDFacetEnum;
import eu.openminted.content.connector.utils.faceting.OMTDFacetLabels;
import eu.openminted.content.service.OmtdNamespace;
import eu.openminted.content.service.rest.ContentServiceController;
import eu.openminted.registry.core.domain.Facet;
import eu.openminted.registry.core.domain.Value;
import eu.openminted.registry.domain.Corpus;
import eu.openminted.registry.domain.PublicationTypeEnum;
import eu.openminted.registry.domain.RightsStatementEnum;
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
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(classes = {ServiceConfiguration.class})
@ContextConfiguration("classpath:/springrest-servlet.xml")
public class FetchMetadataTaskTest {

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
    public void testContentConnectors() {
        System.out.println("Print all available connectors\n");
        for (ContentConnector contentConnector : contentConnectors) {
            System.out.println(contentConnector.getSourceName());
        }
        System.out.println("\n");
    }

    @Test
    @Ignore
    public void testUnwantedValues() {
        query.setParams(new HashMap<>());
        query.getParams().put(OMTDFacetEnum.DOCUMENT_LANG.value(), new ArrayList<>());
        query.getParams().get(OMTDFacetEnum.DOCUMENT_LANG.value()).add("Nl");

        SearchResult searchResult = controller.browse(query);
        if (searchResult != null) {
            searchResult.getFacets().forEach(facet -> facet.getValues().forEach(value -> System.out.println("Facet " + facet.getLabel() + ": " + value.getValue() + ": " + value.getCount())));
        }
    }

    @Test
    @Ignore
    public void testPublicationValues() {

        query.setParams(new HashMap<>());
        query.getParams().put(OMTDFacetEnum.PUBLICATION_TYPE.value(), new ArrayList<>());
        query.getParams().get(OMTDFacetEnum.PUBLICATION_TYPE.value()).add(omtdFacetLabels.getPublicationTypeLabelFromEnum(PublicationTypeEnum.RESEARCH_PROPOSAL));
        query.getParams().get(OMTDFacetEnum.PUBLICATION_TYPE.value()).add(PublicationTypeEnum.BACHELOR_THESIS.toString());

        SearchResult searchResult = controller.browse(query);
        if (searchResult != null) {
            searchResult.getFacets().forEach(facet -> facet.getValues().forEach(value -> System.out.println("Facet " + facet.getLabel() + ": " + value.getValue() + ": " + value.getCount())));
        }
    }

    @Test
    @Ignore
    public void testRightsValues() {

        query.setParams(new HashMap<>());
        query.getParams().put(OMTDFacetEnum.RIGHTS.value(), new ArrayList<>());
        query.getParams().get(OMTDFacetEnum.RIGHTS.value()).add(omtdFacetLabels.getRightsStmtLabelFromEnum(RightsStatementEnum.OPEN_ACCESS));
        query.getParams().get(OMTDFacetEnum.RIGHTS.value()).add(RightsStatementEnum.RESTRICTED_ACCESS.toString());

        SearchResult searchResult = controller.browse(query);
        if (searchResult != null) {
            searchResult.getFacets().forEach(facet -> facet.getValues().forEach(value -> System.out.println("Facet " + facet.getLabel() + ": " + value.getValue() + ": " + value.getCount())));
        }
    }

    @Test
    @Ignore
    public void testLanguageValues() {
        query.setParams(new HashMap<>());
        query.getParams().put(OMTDFacetEnum.DOCUMENT_LANG.value(), new ArrayList<>());
        query.getParams().get(OMTDFacetEnum.DOCUMENT_LANG.value()).add("Nl");

//        Additional parameters for filtering are in comments below

        /*
            query.getParams().put(OMTDFacetEnum.DOCUMENT_TYPE.value(), new ArrayList<>());
            query.getParams().put(OMTDFacetEnum.PUBLICATION_YEAR.value(), new ArrayList<>());
            query.getParams().get("documentlanguage").add("Greek, Modern (1453-)");
            query.getParams().get("documentlanguage").add("Lithuanian");
            query.getParams().get(OMTDFacetEnum.DOCUMENT_TYPE.value()).add(omtdFacetLabels.getDocumentTypeLabelFromEnum(DocumentTypeEnum.WITH_FULL_TEXT));
            query.getParams().get(OMTDFacetEnum.DOCUMENT_TYPE.value()).add(omtdFacetLabels.getDocumentTypeLabelFromEnum(DocumentTypeEnum.WITH_ABSTRACT_ONLY));
            query.getParams().get("documenttype").add("fulltext");
            query.getParams().get("publicationyear").add("2010");
            query.getParams().get("publicationyear").add("1528");
            query.setKeyword("digital");
            query.getParams().get("documentlanguage").add("Czech");
            query.getParams().get("documentlanguage").add("Catalan; Valencian");
            query.getParams().get("documentlanguage").add("English, Middle (1100-1500)");
            query.getParams().get("documentlanguage").add("Greek, Ancient (to 1453)");
            query.getParams().get("documentlanguage").add("Bokmål, Norwegian; Norwegian Bokmål");
            query.getParams().get("documentlanguage").add("French");
            query.getParams().get("documentlanguage").add("Official Aramaic (700-300 BCE); Imperial Aramaic (700-300 BCE)");
            query.getParams().get("documentlanguage").add("Official Aramaic (700-300 Bce); Imperial Aramaic (700-300 Bce)");
        */

//        Example of an openAIRE's solr query
        /*

        start=0&rows=1&facet=true&facet.field=instancetypename
        &facet.field=resultdateofacceptance&facet.field=resultrights
        &facet.field=resultlanguagename
        &fq=resultlanguagename:Bokmål+OR+resultlanguagename:+Norwegian;+Norwegian+Bokmål
        &fq=resulttypename:publication&fq=deletedbyinference:false&fq=resultrights:Open+Access&fl=__result&q=*:*
         */


        SearchResult searchResult = controller.browse(query);

        if (searchResult != null) {
            searchResult.getFacets().forEach(facet -> {
                if (facet.getField().equalsIgnoreCase(OMTDFacetEnum.DOCUMENT_LANG.value()))
                    facet.getValues().forEach(value ->
                            System.out.println("Facet " + facet.getLabel() + ": " + value.getValue() + (value.getLabel() != null ? "/" + value.getLabel() : "") + ": " + value.getCount()));
            });
        }
    }

    @Test
    @Ignore
    public void testPublicationType() {
        query.setParams(new HashMap<>());
        query.getParams().put(OMTDFacetEnum.SOURCE.value(), new ArrayList<>());
        query.getParams().get(OMTDFacetEnum.SOURCE.value()).add("CORE");
        query.getParams().put(OMTDFacetEnum.PUBLICATION_TYPE.value(), new ArrayList<>());
        query.getParams().get(OMTDFacetEnum.PUBLICATION_TYPE.value()).add(PublicationTypeEnum.RESEARCH_ARTICLE.toString());

        SearchResult searchResult = controller.browse(query);

        if (searchResult != null) {
            searchResult.getFacets().forEach(facet -> {
                if (facet.getField().equalsIgnoreCase(OMTDFacetEnum.PUBLICATION_TYPE.value()))
                    facet.getValues().forEach(value ->
                            System.out.println("Facet " + facet.getLabel() + ": " + value.getValue() + (value.getLabel() != null ? "/" + value.getLabel() : "") + ": " + value.getCount()));
            });
        }
    }

    @Test
    @Ignore
    public void testBrowse() {
        query.setParams(new HashMap<>());
        query.getParams().put("source", new ArrayList<>());
        query.getParams().get("source").add("OpenAIRE");

//        Additional parameters for filtering are in comments below

        /*
            query.setKeyword("alekos");

            query.getParams().put("licence", new ArrayList<>());
            query.getParams().get("licence").add("Embargo");
            query.getParams().put("publicationyear", new ArrayList<>());
            query.getParams().get("publicationyear").add("2009");
            query.getParams().get("source").add("CORE");
        */


        SearchResult searchResult = controller.browse(query);
        if (searchResult != null) {

            System.out.println("Results from OpenAIRE: " + searchResult.getTotalHits());

            for (Facet facet : searchResult.getFacets()) {
                for (Value value : facet.getValues()) {
                    System.out.println(value.getValue() + " " + value.getCount());
                }
            }
        }
    }

    @Test
    @Ignore
    public void testBrowseOnlyCORE() {
        query.setParams(new HashMap<>());
        query.getParams().put("source", new ArrayList<>());
        query.getParams().get("source").add("CORE");

        SearchResult searchResult = controller.browse(query);
        if (searchResult != null) {
            System.out.println("Results from CORE: " + searchResult.getTotalHits());
        }
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
    @Ignore
    public void testUser() {
        ResponseEntity<Object> responseEntity = controller.user();
        System.out.println(responseEntity.getBody());
    }

    @Test
    @Ignore
    public void run() throws Exception {

        Query query = new Query();
        query.setFrom(0);
        query.setTo(1);
        query.setParams(new HashMap<>());
        query.getParams().put("licence", new ArrayList<>());
        query.getParams().get("licence").add("Open Access");
        query.getParams().put("source", new ArrayList<>());
        query.getParams().get("source").add("OpenAIRE");
        query.setKeyword("digital");

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        XPath xpath = XPathFactory.newInstance().newXPath();
        xpath.setNamespaceContext(new OmtdNamespace());

        Document currentDoc;
        NodeList nodes;


        if (contentConnectors != null) {
            for (ContentConnector connector : contentConnectors) {
                System.out.println(connector.getSourceName());
                if (connector.getSourceName().equalsIgnoreCase("OpenAIRE")) {
                    SearchResult searchResult = connector.search(query);

                    System.out.println(searchResult.getTotalHits());

                    InputStream inputStream = connector.fetchMetadata(query);


                    currentDoc = dbf.newDocumentBuilder().newDocument();

                    Document doc = dbf.newDocumentBuilder().parse(inputStream);
                    nodes = (NodeList) xpath.evaluate("//OMTDPublications/documentMetadataRecord", doc, XPathConstants.NODESET);

//                    DocumentMetadataRecord documentMetadataRecord = new DocumentMetadataRecord();
//                    documentMetadataRecord.getDocument().getPublication().getDistributions().get(0).getHashkey();
                    int countAbstracts = 0;
                    int countFulltext = 0;
                    if (nodes != null) {
                        for (int i = 0; i < nodes.getLength(); i++) {

                            Node imported = currentDoc.importNode(nodes.item(i), true);
                            XPathExpression identifierExpression = xpath.compile("metadataHeaderInfo/metadataRecordIdentifier/text()");
                            String identifier = (String) identifierExpression.evaluate(imported, XPathConstants.STRING);

//                            if (identifier == null || identifier.isEmpty() || identifier.contains("dedup")) {
//                                System.out.println("MetadataDocument Identifier " + identifier +  " is empty or null or dedup. Skipping current document.");
//                                continue;
//                            } else {
//                                System.out.println(identifier);

                            XPathExpression distributionListExpression = xpath.compile("document/publication/distributions/documentDistributionInfo/hashkey");
                            NodeList hashkeys = (NodeList) distributionListExpression.evaluate(imported, XPathConstants.NODESET);

//                                showXML(imported);

                            boolean hasFulltext = false;

                            if (hashkeys != null && hashkeys.getLength() > 0) {
                                for (int j = 0; j < hashkeys.getLength(); j++) {
                                    Node hashkey = hashkeys.item(j);
                                    if (hashkey != null) {
                                        hasFulltext = true;
                                        System.out.println(hashkey.getTextContent());
                                    }
                                }
                                countFulltext++;
                            } else {
                                hasFulltext = false;
                            }

                            System.out.println(hasFulltext);
                            XPathExpression abstractListExpression = xpath.compile("document/publication/abstracts/abstract");
                            NodeList abstracts = (NodeList) abstractListExpression.evaluate(imported, XPathConstants.NODESET);

                            if (abstracts != null) {
                                StringBuilder abstractText = new StringBuilder();
                                for (int j = 0; j < abstracts.getLength(); j++) {
                                    Node node = abstracts.item(j);
                                    if (node != null)
                                        abstractText.append(node.getTextContent()).append("\n");
                                }
                                countAbstracts++;
//                                    System.out.println(abstractText.toString());
                            }
//                            }
                        }
                        System.out.println("Fulltext: " + countFulltext);
                        System.out.println("Abstracts : " + countAbstracts);
                    }
                }
            }
        }
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

        java.util.regex.Pattern pattern = Pattern.compile(invalidCharacters);
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