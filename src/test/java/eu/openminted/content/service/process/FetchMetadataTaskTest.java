package eu.openminted.content.service.process;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.content.service.OmtdNamespace;
import eu.openminted.content.service.ServiceConfiguration;
import eu.openminted.registry.domain.DocumentMetadataRecord;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.swing.text.AbstractDocument;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ServiceConfiguration.class})
public class FetchMetadataTaskTest {

    @Autowired
    List<ContentConnector> contentConnectors;

    private void showXML(Node node) throws TransformerException, IOException {
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.transform(new DOMSource(node), new StreamResult(System.out));
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

        Document currentDoc = null;
        NodeList nodes = null;



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
//
                                if (hashkeys != null && hashkeys.getLength() > 0) {
                                    for (int j = 0; j < hashkeys.getLength(); j++) {
//                                        Node hashkey = hashkeys.item(j);
//                                        if (hashkey != null) {
//                                            hasFulltext = true;
//                                            System.out.println(hashkey.getTextContent());
//                                        }
                                    }
                                    countFulltext++;
                                } else {
                                    hasFulltext = false;
                                }

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

}