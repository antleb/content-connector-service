package eu.openminted.content.service.extensions;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.store.restclient.StoreRESTClient;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.*;
import java.util.List;

public class FetchMetadataHandler extends DefaultHandler {
    private static Logger log = Logger.getLogger(FetchMetadataHandler.class.getName());

    private String identifier = "";
    private String value = "";
    private File metadataFile;
    private File downloadFile;
    private OutputStream outputStream = null;
    private ContentConnector connector;
    private StoreRESTClient storeRESTClient;
    private String archiveId;
    private List<String> identifiers;

    public File getMetadataFile() {
        return metadataFile;
    }

    public void setMetadataFile(File metadataFile) {
        this.metadataFile = metadataFile;
    }

    public File getDownloadFile() {
        return downloadFile;
    }

    public void setDownloadFile(File downloadFile) {
        this.downloadFile = downloadFile;
    }

    public ContentConnector getConnector() {
        return connector;
    }

    public void setConnector(ContentConnector connector) {
        this.connector = connector;
    }

    public StoreRESTClient getStoreRESTClient() {
        return storeRESTClient;
    }

    public void setStoreRESTClient(StoreRESTClient storeRESTClient) {
        this.storeRESTClient = storeRESTClient;
    }

    public String getArchiveId() {
        return archiveId;
    }

    public void setArchiveId(String archiveId) {
        this.archiveId = archiveId;
    }

    public List<String> getIdentifiers() {
        return identifiers;
    }

    public void setIdentifiers(List<String> identifiers) {
        this.identifiers = identifiers;
    }

    @Override
    public void processingInstruction(String target,
                                      String data) {
        try {
            if (outputStream != null) {
                outputStream.write("<?".getBytes());
                outputStream.write(target.getBytes());
                if (data != null && data.length() > 0) {
                    outputStream.write(" ".getBytes());
                    outputStream.write(data.getBytes());
                }
                outputStream.write("?>\n".getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        try {
            if (qName.equalsIgnoreCase("ns1:documentMetadataRecord")) {
                outputStream = new FileOutputStream(metadataFile, false);
                outputStream.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n".getBytes());
            }
            if (outputStream != null) {
                outputStream.write("<".getBytes());
                outputStream.write(qName.getBytes());
                if (attributes != null) {
                    int numberAttributes = attributes.getLength();
                    for (int loopIndex = 0; loopIndex < numberAttributes;
                         loopIndex++) {
                        outputStream.write(" ".getBytes());
                        outputStream.write(attributes.getQName(loopIndex).getBytes());
                        outputStream.write("=\"".getBytes());
                        outputStream.write(attributes.getValue(loopIndex).getBytes());
                        outputStream.write("\"".getBytes());
                    }
                }
                outputStream.write(">".getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (qName.equalsIgnoreCase("ns1:metadataRecordIdentifier")) identifier = "";
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {

        if (qName.equalsIgnoreCase("ns1:metadataRecordIdentifier")) {
            identifier = value;
            identifiers.add(identifier);
//            new Thread(this::downloadFullText).start();
        }
        if (outputStream != null) {
            try {
                outputStream.write("</".getBytes());
                outputStream.write(qName.getBytes());
                outputStream.write(">\n".getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (qName.equalsIgnoreCase("ns1:documentMetadataRecord")) {
            try {
                outputStream.flush();
                outputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            storeRESTClient.storeFile(metadataFile, archiveId + "/metadata", identifier + ".xml");
        }

    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        value = new String(ch, start, length);
        if (outputStream != null) {
            try {
                outputStream.write(value.trim().getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void downloadFullText() {
        try {
            InputStream fullTextInputStream = connector.downloadFullText(identifier);
            FileOutputStream outputStream = null;
            if (fullTextInputStream != null) {

                outputStream = new FileOutputStream(downloadFile, false);
                IOUtils.copy(fullTextInputStream, outputStream);
                storeRESTClient.storeFile(downloadFile, archiveId + "/documents", identifier + ".pdf");
            }
            IOUtils.closeQuietly(fullTextInputStream);
            IOUtils.closeQuietly(outputStream);
        } catch (FileNotFoundException e) {
            log.error("FetchMetadataHandler.run- Downloading document -FileNotFoundException ", e);
        } catch (IOException e) {
            log.error("FetchMetadataHandler.run- Downloading document -IOException ", e);
        }
    }
}
