package eu.openminted.content.service.process;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.content.service.database.CorpusBuilderInfoDao;
import eu.openminted.content.service.cache.CacheClient;
import eu.openminted.content.service.messages.JMSProducer;
import eu.openminted.content.service.model.CorpusBuilderInfoModel;
import eu.openminted.corpus.CorpusBuildingState;
import eu.openminted.corpus.CorpusStatus;
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
import java.util.*;

public class FetchMetadataTask implements Runnable {
    private static Logger log = Logger.getLogger(FetchMetadataTask.class.getName());
    private ContentConnector connector;
    private Query query;
    private String archiveId;
    private StoreRESTClient storeRESTClient;
    private CacheClient cacheClient;
    private String tempDirectoryPath;
    private InputStream inputStream;
    private CorpusBuilderInfoDao corpusBuilderInfoDao;
    private String corpusId;
    private boolean isInterrupted;
    private int contentLimit;
    private JMSProducer producer;
    private CorpusBuildingState corpusBuildingState;

    public InputStream getInputStream() {
        return inputStream;
    }

    public void setCorpusBuilderInfoDao(CorpusBuilderInfoDao corpusBuilderInfoDao) {
        this.corpusBuilderInfoDao = corpusBuilderInfoDao;
    }

    public void setCorpusId(String corpusId) {
        this.corpusId = corpusId;
    }

    public FetchMetadataTask(StoreRESTClient storeRESTClient,
                             String authenticationToken,
                             ContentConnector connector,
                             Query query,
                             String tempDirectoryPath,
                             String archiveId,
                             int contentLimit,
                             JMSProducer producer) {
        this.connector = connector;
        this.query = query;
        this.archiveId = archiveId;
        this.storeRESTClient = storeRESTClient;
        this.tempDirectoryPath = tempDirectoryPath;
        this.contentLimit = contentLimit;
        this.producer = producer;
        this.corpusBuildingState = new CorpusBuildingState();
        this.corpusBuildingState.setTotalHits(0);
        this.corpusBuildingState.setTotalFulltext(0);
        this.corpusBuildingState.setMetadataProgress(0);
        this.corpusBuildingState.setFulltextProgress(0);
        this.corpusBuildingState.setConnector(this.connector.getSourceName());
        this.corpusBuildingState.setToken(authenticationToken);
    }

    @Override
    public void run() {
        tempDirectoryPath = tempDirectoryPath.replaceAll("/$", "");
        String archivePath = tempDirectoryPath + "/" + archiveId + "/" + connector.getSourceName();

        File archive = new File(archivePath);
        if (archive.mkdirs()) log.debug("Creating " + archivePath + " directory");
        File metadataFile = new File(archive.getPath() + "/" + archiveId + ".xml");
        File downloadFile = new File(archive.getPath() + "/" + archiveId + ".pdf");
        File abstractFile = new File(archive.getPath() + "/" + archiveId + ".txt");
        Map<String, List<String>> identifiers = new HashMap<>();

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        XPath xpath = XPathFactory.newInstance().newXPath();

        Document currentDoc = null;
        NodeList nodes = null;

        final long period = (long) 10000;

        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                CorpusBuilderInfoModel corpusBuilderInfoModel = corpusBuilderInfoDao.find(corpusId);
                if (corpusBuilderInfoModel != null
                        && corpusBuilderInfoModel.getStatus().equalsIgnoreCase(CorpusStatus.CANCELED.toString())) {
                    IOUtils.closeQuietly(inputStream);
                    isInterrupted = true;
                }

                if (isInterrupted
                        || (corpusBuilderInfoModel != null
                        && corpusBuilderInfoModel.getStatus().equalsIgnoreCase(CorpusStatus.CREATED.toString()))
                        || corpusBuilderInfoModel == null) {
                    this.cancel();
                }
            }
        };

        Timer timer = new Timer(true);
        timer.schedule(timerTask, 0, period);

        try {
            CorpusBuilderInfoModel corpusBuilderInfoModel = corpusBuilderInfoDao.find(corpusId);
            corpusBuildingState.setId(corpusId + "@" + connector.getSourceName());
            corpusBuildingState.setCurrentStatus(corpusBuilderInfoModel.getStatus());
            SearchResult searchResult = connector.search(query);
            if (searchResult != null) {
                corpusBuildingState.setTotalHits(searchResult.getTotalHits());
                corpusBuildingState.setMetadataProgress(0);
            }

            currentDoc = dbf.newDocumentBuilder().newDocument();
            inputStream = connector.fetchMetadata(query);

            Document doc = dbf.newDocumentBuilder().parse(inputStream);
            nodes = (NodeList) xpath.evaluate("//OMTDPublications/documentMetadataRecord", doc, XPathConstants.NODESET);
        } catch (ParserConfigurationException e) {
            log.error("FetchMetadataTask.run-ParserConfigurationException ", e);
        } catch (IOException e) {
            log.info("Fetching metadata has been interrupted");
            log.debug("FetchMetadataTask.run-IOException ", e);
        } catch (SAXException e) {
            log.info("Fetching metadata has been interrupted");
            log.debug("FetchMetadataTask.run-SAXException ", e);
        } catch (XPathExpressionException e) {
            log.error("FetchMetadataTask.run-XPathExpressionException ", e);
        }

        int totalFulltext = 0;
        int totalRejected = 0;
        int metadataProgress = 0;
        int fulltextProgress = 0;

        if (nodes != null) {
            corpusBuildingState.setCurrentStatus(CorpusStatus.PROCESSING_METADATA.toString());
            producer.sendMessage(CorpusBuildingState.class.toString(), corpusBuildingState);

            for (int i = 0; i < nodes.getLength(); i++) {
                if (isInterrupted) break;
                if (contentLimit > 0 && contentLimit <= metadataProgress) break;

                Node imported = currentDoc.importNode(nodes.item(i), true);
                XPathExpression identifierExpression;
                try {
                    identifierExpression = xpath.compile("metadataHeaderInfo/metadataRecordIdentifier/text()");
                    String identifier = (String) identifierExpression.evaluate(imported, XPathConstants.STRING);

                    if (identifier == null || identifier.isEmpty() || identifier.contains("dedup")) {
                        totalRejected++;
                        log.debug("MetadataDocument Identifier is empty or null or dedup. Skipping current document.");
                        continue;
                    } else {
                        // Find hashkeys from imported node
                        XPathExpression distributionListExpression = xpath.compile("document/publication/distributions/documentDistributionInfo/hashkey");
                        NodeList hashkeys = (NodeList) distributionListExpression.evaluate(imported, XPathConstants.NODESET);

                        boolean hasFulltext = false;

                        if (hashkeys != null && hashkeys.getLength() > 0) {
                            for (int j = 0; j < hashkeys.getLength(); j++) {
                                Node hashkey = hashkeys.item(j);
                                if (hashkey != null) {
                                    hasFulltext = true;

                                    if (!identifiers.keySet().contains(identifier))
                                        identifiers.put(identifier, new ArrayList<>());

                                    identifiers.get(identifier).add(hashkey.getTextContent());
                                    totalFulltext++;
                                }
                            }
                        } else {
                            hasFulltext = false;
                        }

                        if (!hasFulltext) {
                            log.debug("MetadataDocument Identifier does not contain fulltext. Skipping current document.");
                            totalRejected++;
                            continue;
                        }
                    }

                    writeToFile(imported, metadataFile);
                    storeRESTClient.storeFile(metadataFile, archiveId + "/metadata", identifier + ".xml");

                    // Find Abstracts from imported node
                    XPathExpression abstractListExpression = xpath.compile("document/publication/abstracts/abstract");
                    NodeList abstracts = (NodeList) abstractListExpression.evaluate(imported, XPathConstants.NODESET);

                    if (abstracts != null) {
                        StringBuilder abstractText = new StringBuilder();
                        for (int j = 0; j < abstracts.getLength(); j++) {
                            Node node = abstracts.item(j);
                            if (node != null)
                                abstractText.append(node.getTextContent()).append("\n");
                        }

                        FileWriter fileWriter = new FileWriter(abstractFile);
                        fileWriter.write(abstractText.toString());
                        fileWriter.flush();
                        fileWriter.close();
                        storeRESTClient.storeFile(abstractFile, archiveId + "/abstract", identifier + ".txt");
                    }

                    metadataProgress++;
                    corpusBuildingState.setTotalRejected(totalRejected);
                    corpusBuildingState.setMetadataProgress(metadataProgress);
                    if (metadataProgress > 0 && metadataProgress % 10 == 0) {
                        producer.sendMessage(CorpusBuildingState.class.toString(), corpusBuildingState);
                    }
                } catch (XPathExpressionException e) {
                    log.error("FetchMetadataTask.run-Fetching Metadata -XPathExpressionException ", e);
                } catch (IOException e) {
                    log.error("FetchMetadataTask.run-Fetching Metadata -IOException ", e);
                }
            }

            try {
                producer.sendMessage(CorpusBuildingState.class.toString(), corpusBuildingState);
                // closing previous stream
                IOUtils.closeQuietly(inputStream);
                corpusBuildingState.setTotalFulltext(totalFulltext);
                corpusBuildingState.setCurrentStatus(CorpusStatus.PROCESSING_FULLTEXT.toString());
                producer.sendMessage(CorpusBuildingState.class.toString(), corpusBuildingState);

                for (String identifier : identifiers.keySet()) {
                    if (isInterrupted) break;
                    if (identifiers.get(identifier).size() > 0) {
                        for (String hashKey : identifiers.get(identifier)) {
                            if (hashKey != null && !hashKey.isEmpty()) {
                                InputStream fullTextInputStream = cacheClient.getDocument(connector, identifier, hashKey);
                                FileOutputStream outputStream = null;
                                if (fullTextInputStream != null) {

                                    outputStream = new FileOutputStream(downloadFile, false);
                                    IOUtils.copy(fullTextInputStream, outputStream);
                                    storeRESTClient.storeFile(downloadFile, archiveId + "/fulltext", identifier + ".pdf");
                                    fulltextProgress++;
                                    corpusBuildingState.setFulltextProgress(fulltextProgress);
                                    if (totalFulltext > 0 && fulltextProgress % 10 == 0) {
                                        producer.sendMessage(CorpusBuildingState.class.toString(), corpusBuildingState);
                                    }
                                }
                                IOUtils.closeQuietly(fullTextInputStream);
                                IOUtils.closeQuietly(outputStream);
                            }
                        }
                    }
                }

                corpusBuildingState.setCurrentStatus(CorpusStatus.CREATED.toString());
                producer.sendMessage(CorpusBuildingState.class.toString(), corpusBuildingState);
            } catch (FileNotFoundException e) {
                log.error("FetchMetadataTask.run- Downloading fulltext -FileNotFoundException ", e);
            } catch (IOException e) {
                log.error("FetchMetadataTask.run- Downloading fulltext -IOException ", e);
            }
        }

        // Close timer for cancelling process
        timer.cancel();
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

    public void setCacheClient(CacheClient cacheClient) {
        this.cacheClient = cacheClient;
    }

}
