package eu.openminted.content.service.extensions;

import eu.openminted.store.restclient.StoreRESTClient;
import org.apache.commons.io.IOUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by constantine on 12/01/2017.
 */
public class MetadataDefaultHandler extends DefaultHandler {
    File file;
    FileOutputStream outputStream;

    String archiveId;
    String connectorName;
    String metadataRecordIdentifier;
    String value;
    InputStream inputStream;
    StoreRESTClient storeRESTClient;

    public MetadataDefaultHandler(String archiveId, String connectorName, InputStream inputStream, StoreRESTClient storeRESTClient) {
        try {
            this.archiveId = archiveId;
            this.connectorName = connectorName;
            this.inputStream = inputStream;
            this.storeRESTClient = storeRESTClient;
            file = new File("/Users/constantine/" + archiveId + "/" + connectorName + ".xml");

            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        if (qName.equalsIgnoreCase("documentMetadataRecord")) {
            try {

                outputStream = new FileOutputStream(file, false);
                IOUtils.copy(inputStream, outputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (qName.equalsIgnoreCase("metadataRecordIdentifier")) {
            metadataRecordIdentifier = value;
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        try {
            outputStream.flush();
            IOUtils.closeQuietly(outputStream);
            storeRESTClient.updload(file, archiveId, metadataRecordIdentifier + ".xml");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        value = new String(ch, start, length);
    }
}
