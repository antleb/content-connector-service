package eu.openminted.content.service.extensions;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.store.restclient.StoreRESTClient;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.springframework.beans.factory.annotation.Autowired;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.*;

/**
 * Created by constantine on 08/01/2017.
 */
public class FetchMetadataTask implements Runnable {
    private ContentConnector connector;
    private Query query;
    private String archiveId;
    StoreRESTClient storeRESTClient;

    public FetchMetadataTask(StoreRESTClient storeRESTClient, ContentConnector connector, Query query, String archiveId) {
        this.connector = connector;
        this.query = query;
        this.archiveId = archiveId;
        this.storeRESTClient = storeRESTClient;
    }

    @Override
    public void run() {
        InputStream inputStream = connector.fetchMetadata(query);
        System.out.println("Fetching metadata from " + connector.getSourceName());
//        javax.xml.parsers.DocumentBuilderFactory dbFactory = javax.xml.parsers.DocumentBuilderFactory.newInstance();
        try {
//            javax.xml.parsers.DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
//            org.w3c.dom.Document doc = dBuilder.parse(inputStream);
//            doc.getDocumentElement().normalize();
//            XPath xpath = XPathFactory.newInstance().newXPath();


            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();
            saxParser.parse(inputStream, new MetadataDefaultHandler(archiveId, connector.getSourceName(), inputStream, storeRESTClient));


//            File file = new File("/Users/constantine/" + archiveId + "/" + connector.getSourceName() + "_mytemp.xml");
//            file.setWritable(true);
//            file.createNewFile();
//            FileOutputStream outputStream = new FileOutputStream(file, true);

//            org.w3c.dom.NodeList nodeList = doc.getElementsByTagName("documentMetadataRecordType");
//            for(int i=0;i<nodeList.getLength();i++){
//                org.w3c.dom.Node node = nodeList.item(i);
//                outputStream.write(xpath.evaluate("documentMetadataRecordType/metadataHeaderInfo/metadataRecordIdentifier/text()", node, XPathConstants.STRING).toString().getBytes());
////                fs.write(xpath.evaluate("documentMetadataRecordType/metadataHeaderInfo/metadataRecordIdentifier/text()", node, XPathConstants.STRING).toString().getBytes());
//            }

            // write output to file here
//            IOUtils.copy(inputStream, outputStream);
//            outputStream.flush();
//            IOUtils.closeQuietly(outputStream);
//            IOUtils.closeQuietly(inputStream);
//            storeRESTClient.updload(file, archiveId, connector.getSourceName() + "_mytemp.xml");
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
//        } catch (XPathExpressionException e) {
//            e.printStackTrace();
        }

//        String line;
//        StringBuilder stringBuilder = new StringBuilder();
//        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
//        try {
//            while ((line = br.readLine()) != null) {
//                if (line.toLowerCase().contains("</documentmetadatarecord>")) {
//                    System.out.println(line);
//                    stringBuilder.delete(0, stringBuilder.length());
//                }
//                else {
//                    stringBuilder.append(line);
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                br.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
        /*
        InputStream fetches metadata
        for each metadata
            store metadata to metadata subarchive with name documentId.xml
            call downloadFullText by the documentId
            for each fulltext result
            store each result to fulltext subarchive with name $documentId
        */
    }
}
