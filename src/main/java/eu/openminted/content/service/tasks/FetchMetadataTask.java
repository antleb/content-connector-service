package eu.openminted.content.service.tasks;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.store.restclient.StoreRESTClient;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FetchMetadataTask implements Runnable {
    private static Logger log = Logger.getLogger(FetchMetadataTask.class.getName());
    private ContentConnector connector;
    private Query query;
    private String archiveId;
    private StoreRESTClient storeRESTClient;
    private String tempDirectoryPath;

    public FetchMetadataTask(StoreRESTClient storeRESTClient, ContentConnector connector, Query query, String tempDirectoryPath, String archiveId) {
        this.connector = connector;
        this.query = query;
        this.archiveId = archiveId;
        this.storeRESTClient = storeRESTClient;
        this.tempDirectoryPath = tempDirectoryPath;
    }

    @Override
    public void run() {
        InputStream inputStream = null;
        tempDirectoryPath = tempDirectoryPath.replaceAll("/$", "");
        String archivePath = tempDirectoryPath + "/" + archiveId + "/" + connector.getSourceName();

//*
        File archive = new File(archivePath);
        if (archive.mkdirs()) log.debug("Creating " + archivePath + " directory");
        File metadataFile = new File(archive.getPath() + "/" + archiveId + ".xml");
        File downloadFile = new File(archive.getPath() + "/" + archiveId + ".pdf");
        List<String> identifiers = new ArrayList<>();

/*
        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser saxParser;

        try {

            saxParser = factory.newSAXParser();
            inputStream = connector.fetchMetadata(query);
            FetchMetadataHandler handler = new FetchMetadataHandler();
            handler.setMetadataFile(metadataFile);
            handler.setDownloadFile(downloadFile);
            handler.setConnector(connector);
            handler.setStoreRESTClient(storeRESTClient);
            handler.setArchiveId(this.archiveId);
            handler.setIdentifiers(identifiers);

            saxParser.parse(inputStream, handler);
        } catch (SAXException e) {
            log.error("SAXException", e);
        } catch (IOException e) {
            log.error("IOException", e);
        } catch (ParserConfigurationException e) {
            log.error("ParserConfigurationException", e);
        }
    */


        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        XPath xpath = XPathFactory.newInstance().newXPath();

        Document currentDoc = null;
        NodeList nodes = null;

        try {
            currentDoc = dbf.newDocumentBuilder().newDocument();
            inputStream = connector.fetchMetadata(query);
            Document doc = dbf.newDocumentBuilder().parse(inputStream);
            nodes = (NodeList) xpath.evaluate("//OMTDPublications/documentMetadataRecord", doc, XPathConstants.NODESET);
        } catch (ParserConfigurationException e) {
            log.error("FetchMetadataTask.run-ParserConfigurationException ", e);
        } catch (IOException e) {
            log.error("FetchMetadataTask.run-IOException ", e);
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (XPathExpressionException e) {
            log.error("FetchMetadataTask.run-XPathExpressionException ", e);
        }

        if (nodes != null) {
            for (int i = 0; i < nodes.getLength(); i++) {
                Node imported = currentDoc.importNode(nodes.item(i), true);
                XPathExpression identifierExpression;
                try {
                    identifierExpression = xpath.compile("metadataHeaderInfo/metadataRecordIdentifier/text()");
                    String identifier = (String) identifierExpression.evaluate(imported, XPathConstants.STRING);

                    if (identifier == null || identifier.isEmpty()) {
                        log.info("MetadataDocument Identifier is empty or null. Skipping current document.");
                        continue;
                    } else {
                        identifiers.add(identifier);
                    }

                    writeToFile(imported, metadataFile);
                    storeRESTClient.updload(metadataFile, archiveId + "/metadata", identifier + ".xml");

                } catch (XPathExpressionException e) {
                    log.error("FetchMetadataTask.run-Fetching Metadata -XPathExpressionException ", e);
                }
            }

            IOUtils.closeQuietly(inputStream);

            for (String identifier : identifiers) {
                try {
                    InputStream fullTextInputStream = connector.downloadFullText(identifier);
                    FileOutputStream outputStream = null;
                    if (fullTextInputStream != null) {

                        outputStream = new FileOutputStream(downloadFile, false);
                        IOUtils.copy(fullTextInputStream, outputStream);
                        storeRESTClient.updload(downloadFile, archiveId + "/documents", identifier + ".pdf");
                    }
                    IOUtils.closeQuietly(fullTextInputStream);
                    IOUtils.closeQuietly(outputStream);
                } catch (FileNotFoundException e) {
                    log.error("FetchMetadataTask.run- Downloading document -FileNotFoundException ", e);
                } catch (IOException e) {
                    log.error("FetchMetadataTask.run- Downloading document -IOException ", e);
                }
            }
        }

        storeRESTClient.finalizeArchive(archiveId);


// next line shoul be commited in case of defaultHandler
//        IOUtils.closeQuietly(inputStream);
        if (metadataFile.delete()) log.debug("Removing temp metadata file");
        if (downloadFile.delete()) log.debug("Removing temp download file");
        if (archive.delete()) log.debug("Removing temp directory");
    }

    private void writeToFile(Node node, File file) {
        Transformer transformer;
        try {
            transformer = TransformerFactory.newInstance().newTransformer();
            transformer.transform(new DOMSource(node), new StreamResult(new FileWriter(file)));
        } catch (TransformerConfigurationException e) {
            log.error("FetchMetadataTask.writeToFile-TransformerConfigurationException ", e);
        } catch (IOException e) {
            log.error("FetchMetadataTask.writeToFile-IOException ", e);
        } catch (TransformerException e) {
            log.error("FetchMetadataTask.writeToFile-TransformerException ", e);
        }
    }
}
