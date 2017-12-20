package eu.openminted.content.service;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.content.connector.utils.faceting.OMTDFacetEnum;
import eu.openminted.content.connector.utils.faceting.OMTDFacetLabels;
import eu.openminted.content.service.rest.ContentServiceController;
import eu.openminted.registry.core.domain.Facet;
import eu.openminted.registry.core.domain.Value;
import eu.openminted.registry.domain.DocumentTypeEnum;
import eu.openminted.registry.domain.PublicationTypeEnum;
import eu.openminted.registry.domain.RightsStatementEnum;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
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
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration("classpath:/springrest-servlet.xml")
@ContextConfiguration(classes = { ServiceConfiguration.class})
public class ContentServiceBrowseTest {

    @Autowired
    private List<ContentConnector> contentConnectors;

    @Autowired
    private ContentServiceController controller;

    @Autowired
    private OMTDFacetLabels omtdFacetLabels;

    @Autowired
    private Environment environment;

    private Query query;

    @Before
    public void init() {
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
    public void testUnwantedValues() throws IOException {
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
    public void testPublicationValues() throws IOException {

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
    public void testRightsValues() throws IOException {

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
    public void testLanguageValues() throws IOException {
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
    public void testPublicationType() throws IOException {
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
    public void testBrowse() throws IOException {
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
    public void testBrowseOnlyCORE() throws IOException {
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

    @Test
    @Ignore
    public void contentServiceStatusTest() {
        ServiceStatus serviceStatus = controller.status();

        System.out.println(serviceStatus.getContentConnectors());
    }


    @Test
    @Ignore
    public void testPublicationAndDocumentTypes() throws IOException {
        query.setKeyword("mouse");
        query.setParams(new HashMap<>());
        query.getParams().put(OMTDFacetEnum.DOCUMENT_LANG.value(), new ArrayList<>());
        query.getParams().get(OMTDFacetEnum.DOCUMENT_LANG.value()).add("En");
        query.getParams().put(OMTDFacetEnum.PUBLICATION_YEAR.value(), new ArrayList<>());
        query.getParams().get(OMTDFacetEnum.PUBLICATION_YEAR.value()).add("2015");
        query.getParams().put(OMTDFacetEnum.DOCUMENT_TYPE.value(), new ArrayList<>());
        query.getParams().get(OMTDFacetEnum.DOCUMENT_TYPE.value()).add(omtdFacetLabels.getDocumentTypeLabelFromEnum(DocumentTypeEnum.WITH_FULL_TEXT));
        query.getParams().put(OMTDFacetEnum.PUBLICATION_TYPE.value(), new ArrayList<>());
        query.getParams().get(OMTDFacetEnum.PUBLICATION_TYPE.value()).add(omtdFacetLabels.getPublicationTypeLabelFromEnum(PublicationTypeEnum.RESEARCH_ARTICLE));

        SearchResult searchResult = controller.browse(query);

        if (searchResult.getPublications() != null) {
            for (String metadataRecord : searchResult.getPublications()) {
                System.out.println(metadataRecord);
            }

            for (Facet facet : searchResult.getFacets()) {
                int totalHits = 0;
                System.out.println("facet:{" + facet.getLabel() + "[");
                for (Value value : facet.getValues()) {
                    System.out.println("\t{" + value.getValue() + ":" + value.getCount() + "}");
                    totalHits += value.getCount();
                }
                System.out.println("]}");
                assert totalHits == searchResult.getTotalHits();
            }

            System.out.println("reading " + searchResult.getPublications().size() +
                    " publications from " + searchResult.getFrom() +
                    " to " + searchResult.getTo() +
                    " out of " + searchResult.getTotalHits() + " total hits.");
        } else {
            System.out.println("Could not find any result with these parameters or keyword");
        }

    }

    private void showXML(Node node) throws TransformerException, IOException {
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.transform(new DOMSource(node), new StreamResult(System.out));
    }
}