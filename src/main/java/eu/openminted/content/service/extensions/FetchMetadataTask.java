package eu.openminted.content.service.extensions;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.*;

/**
 * Created by constantine on 08/01/2017.
 */
public class FetchMetadataTask implements Runnable {
    ContentConnector connector;
    Query query;
    public FetchMetadataTask(ContentConnector connector, Query query) {
        this.connector = connector;
        this.query = query;
    }

    @Override
    public void run() {
//        InputStream inputStream = connector.fetchMetadata(query);
        System.out.println("Fetching metadata from " + connector.getSourceName());
        javax.xml.parsers.DocumentBuilderFactory dbFactory = javax.xml.parsers.DocumentBuilderFactory.newInstance();
        File f = new File(connector.getSourceName() + "_mytemp.xml");
        f.setWritable(true);
        FileOutputStream fs;
        try {
            fs = new FileOutputStream(f);
            fs.write("hellllloooooooo\n\n\n\n".getBytes());
            fs.flush();

//            javax.xml.parsers.DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
//            org.w3c.dom.Document doc = dBuilder.parse(inputStream);
//            doc.getDocumentElement().normalize();
//            XPath xpath = XPathFactory.newInstance().newXPath();
//
//            org.w3c.dom.NodeList nodeList = doc.getElementsByTagName("documentMetadataRecordType");
//            for(int i=0;i<nodeList.getLength();i++){
//                org.w3c.dom.Node node = nodeList.item(i);
//
//                fs.write(xpath.evaluate("documentMetadataRecordType/metadataHeaderInfo/metadataRecordIdentifier/text()", node, XPathConstants.STRING).toString().getBytes());
//            }
//            fs.flush();
            fs.close();
//        } catch (ParserConfigurationException e) {
//            e.printStackTrace();
//        } catch (SAXException e) {
//            e.printStackTrace();
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
