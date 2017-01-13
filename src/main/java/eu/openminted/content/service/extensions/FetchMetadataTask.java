package eu.openminted.content.service.extensions;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.store.restclient.StoreRESTClient;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.util.UUID;

/**
 * Created by constantine on 08/01/2017.
 */
public class FetchMetadataTask implements Runnable {
    private static Logger log = Logger.getLogger(FetchMetadataTask.class.getName());
    private ContentConnector connector;
    private Query query;
    private String archiveId;
    private StoreRESTClient storeRESTClient;
    private String tempArchivalPath;

    public FetchMetadataTask(StoreRESTClient storeRESTClient, ContentConnector connector, Query query, String tempArchivalPath, String archiveId) {
        this.connector = connector;
        this.query = query;
        this.archiveId = archiveId;
        this.storeRESTClient = storeRESTClient;
        this.tempArchivalPath = tempArchivalPath;
    }

    @Override
    public void run() {
        InputStream inputStream = null;
        tempArchivalPath.replaceAll("/$", "");
        File archive = new File(tempArchivalPath + "/" + connector.getSourceName());
        archive.mkdirs();
        File file = new File( archive.getPath() + "/" + archiveId + ".xml");
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            XPath xpath = XPathFactory.newInstance().newXPath();
            Document currentDoc = dbf.newDocumentBuilder().newDocument();

            inputStream = connector.fetchMetadata(query);
            Document doc = dbf.newDocumentBuilder().parse(inputStream);
            NodeList nodes = (NodeList) xpath.evaluate("//OMTDPublications/documentMetadataRecord", doc, XPathConstants.NODESET);

            for (int i = 0; i < nodes.getLength(); i++) {
                Node imported = currentDoc.importNode(nodes.item(i), true);
                XPathExpression identifierExpression = xpath.compile("metadataHeaderInfo/metadataRecordIdentifier/text()");
                String identifier = (String) identifierExpression.evaluate(imported, XPathConstants.STRING);

                if (identifier == null || identifier.isEmpty()) {
                    identifier = "Unidentified_" + UUID.randomUUID().toString();
                }
                writeToFile(imported, file);
                storeRESTClient.updload(file, archiveId, identifier + ".xml");
            }
        } catch (Exception e) {
            log.error(e);
        } finally {
            if (inputStream != null)
                IOUtils.closeQuietly(inputStream);
            if (file.delete()) log.debug("Removing temp metadata file");
            if (archive.delete()) log.debug("Removing temp archive directory");
        }
    }

    private void writeToFile(Node node, File file) throws Exception {
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.transform(new DOMSource(node), new StreamResult(new FileWriter(file)));
    }
}
