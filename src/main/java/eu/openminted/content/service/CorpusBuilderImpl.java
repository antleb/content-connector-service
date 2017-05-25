package eu.openminted.content.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openminted.content.connector.*;
import eu.openminted.content.service.dao.CorpusBuilderInfoDao;
import eu.openminted.content.service.extensions.CorpusBuilderExecutionQueueConsumer;
import eu.openminted.content.service.extensions.JMSConsumer;
import eu.openminted.content.service.extensions.JMSProducer;
import eu.openminted.content.service.model.CorpusBuilderInfoModel;
import eu.openminted.corpus.CorpusBuilder;
import eu.openminted.corpus.CorpusStatus;
import eu.openminted.registry.domain.*;
import eu.openminted.store.restclient.StoreRESTClient;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

@Component
@ComponentScan("eu.openminted.content")
public class CorpusBuilderImpl implements CorpusBuilder {
    private static Logger log = Logger.getLogger(CorpusBuilderImpl.class.getName());

    @Autowired(required = false)
    private List<ContentConnector> contentConnectors;

    @Autowired
    private CorpusBuilderInfoDao corpusBuilderInfoDao;

    @Autowired
    private ThreadPoolExecutor threadPoolExecutor;

    @Autowired
    private StoreRESTClient storeRESTClient;

    @Autowired
    private JMSProducer producer;

    @Autowired
    private JMSConsumer consumer;

    @Autowired
    private CorpusBuilderExecutionQueueConsumer corpusBuilderExecutionQueueConsumer;

    @org.springframework.beans.factory.annotation.Value("${tempDirectoryPath}")
    private String tempDirectoryPath;

    @org.springframework.beans.factory.annotation.Value("${store.host}")
    private String storeHost;

    @org.springframework.beans.factory.annotation.Value("${registry.host}")
    private String registryHost;

    private BlockingQueue<Corpus> corpora = new LinkedBlockingQueue<>();

    /**
     * Spring method for initializing the consumer of the corpora queue
     */
    @PostConstruct
    public void init() {
        new Thread(() -> {
            try {
                corpusBuilderExecutionQueueConsumer.init(corpora);
            } catch (Exception ex) {
                log.error("Error executing queue consumer");
            }
        }).start();

        new Thread(()->consumer.listen()).start();
    }

    /**
     *   Method for preparing the corpus building process for a new corpus
     * @param query the query as inserted in the prepare method of the controller
     * @return the Coprus that the user is building
     */
    @Override
    public Corpus prepareCorpus(Query query) {

        // if registryHost ends with '/' remove it
        registryHost = registryHost.replaceAll("/$", "");

        Corpus corpusMetadata = new Corpus();
        String queryString = "";

        // tempQuery is used to import necessary information about the metadata and include them into the new corpus
        Query tempQuery = new Query(query.getKeyword(), query.getParams(), new ArrayList<>(), 0, 1);

        CorpusInfo corpusInfo = new CorpusInfo();
        MetadataHeaderInfo metadataHeaderInfo = new MetadataHeaderInfo();

        // corpusInfo elements
        IdentificationInfo identificationInfo = new IdentificationInfo();
        identificationInfo.setResourceNames(new ArrayList<>());

        // metadataHeaderInfo elements
        MetadataIdentifier metadataIdentifier = new MetadataIdentifier();

        metadataIdentifier.setValue(createCorpusId());
        metadataIdentifier.setMetadataIdentifierSchemeName(MetadataIdentifierSchemeNameEnum.OTHER);
        metadataHeaderInfo.setMetadataRecordIdentifier(metadataIdentifier);

        metadataHeaderInfo.setUserQuery(tempQuery.getKeyword());
        corpusInfo.setDistributionInfos(new ArrayList<>());

        Facet sourceFacet = new Facet();
        SearchResult result = new SearchResult();
        result.setFacets(new ArrayList<>());
        sourceFacet.setField("source");
        sourceFacet.setLabel("Content Source");
        sourceFacet.setValues(new ArrayList<>());

        if (contentConnectors != null) {

            tempQuery.getFacets().add("licence");
            tempQuery.getFacets().add("documentType");
            tempQuery.getFacets().add("documentLanguage");

            for (ContentConnector connector : contentConnectors) {
                SearchResult res = connector.search(tempQuery);
                Value value = new Value();

                value.setValue(connector.getSourceName());
                value.setCount(res.getTotalHits());

                sourceFacet.getValues().add(value);

                result.merge(res);
            }
        }

        result.getFacets().add(sourceFacet);

        for (Facet facet : result.getFacets()) {
            // language
            if (facet.getField().equalsIgnoreCase("documentLanguage")) {
                CorpusTextPartInfo corpusTextPartInfo = new CorpusTextPartInfo();
                CorpusMediaPartsType corpusMediaPartsType = new CorpusMediaPartsType();
                RawCorpusInfo rawCorpusInfo = new RawCorpusInfo();
                CorpusSubtypeSpecificInfo corpusSubtypeSpecificInfo = new CorpusSubtypeSpecificInfo();

                rawCorpusInfo.setCorpusMediaPartsType(corpusMediaPartsType);
                corpusSubtypeSpecificInfo.setRawCorpusInfo(rawCorpusInfo);
                corpusInfo.setCorpusSubtypeSpecificInfo(corpusSubtypeSpecificInfo);

                corpusInfo.getCorpusSubtypeSpecificInfo().getRawCorpusInfo().getCorpusMediaPartsType().getCorpusTextParts().add(corpusTextPartInfo);


                for (Value value : facet.getValues()) {
                    if (value.getCount() > 0) {
                        Language language = new Language();
                        LanguageInfo languageInfo = new LanguageInfo();

                        String name = value.getValue();
                        if (LanguageConverter.getInstance().getOpenaireToOMTDName().containsKey(name)) {
                            name = LanguageConverter.getInstance().getOpenaireToOMTDName().get(name);
                        }

                        String code = LanguageConverter.getInstance().getLangNameToCode().get(name);
                        language.setLanguageTag(name);
                        language.setLanguageId(code);
                        languageInfo.setLanguage(language);

                        corpusTextPartInfo.getLanguages().add(languageInfo);
                    }
                }
            }

            // licence
            if (facet.getField().equalsIgnoreCase("licence")) {
                for (Value value : facet.getValues()) {
                    if (value.getCount() > 0) {
                        DatasetDistributionInfo datasetDistributionInfo = new DatasetDistributionInfo();
                        RightsInfo rightsInfo = new RightsInfo();
                        LicenceInfo licenceInfo = new LicenceInfo();
                        licenceInfo.setLicence(LicenceEnum.NON_STANDARD_LICENCE_TERMS);
                        licenceInfo.setNonStandardLicenceTermsURL(value.getValue());

                        LicenceInfos licenceInfos = new LicenceInfos();
                        licenceInfos.getLicenceInfo().add(licenceInfo);

//                        RightsStatementInfo rightsStatementInfo = new RightsStatementInfo();
//                        rightsStatementInfo.setRightsStmtURL("http://api.openaire.eu/vocabularies/dnet:access_modes");
//                        rightsStatementInfo.setRightsStmtName(RightsStmtNameConverter.convert(value.getValue()));

                        rightsInfo.getLicenceInfos().add(licenceInfos);
                        rightsInfo.getRightsStatement().add(RightsStmtNameConverter.convert(value.getValue()));
//                        rightsInfo.setRightsStatementInfo(rightsStatementInfo);

                        datasetDistributionInfo.setRightsInfo(rightsInfo);
                        corpusInfo.getDistributionInfos().add(datasetDistributionInfo);
                    }
                }
            }
        }

        corpusInfo.setIdentificationInfo(identificationInfo);

        corpusMetadata.setCorpusInfo(corpusInfo);
        corpusMetadata.setMetadataHeaderInfo(metadataHeaderInfo);

        try {
            queryString = new ObjectMapper().writeValueAsString(query);


        } catch (JsonProcessingException e) {
            log.error("CorpusBuilderImpl.prepareCorpus: Unable to write value as String", e);
        }

        if (!queryString.isEmpty()) {
            String archiveID = storeRESTClient.createArchive().getResponse();
            DatasetDistributionInfo datasetDistributionInfo = new DatasetDistributionInfo();
            List<DatasetDistributionInfo> distributionInfos = new ArrayList<>();
            List<DistributionMediumEnum> distributionMediums = new ArrayList<>();

            distributionMediums.add(DistributionMediumEnum.DOWNLOADABLE);
            datasetDistributionInfo.setDistributionMediums(distributionMediums);

//            List<String> dowloadaURLs = new ArrayList<>();
//            dowloadaURLs.add(registryHost + "/omtd-registry/request/corpus/download?archiveId=" + archiveID);
            datasetDistributionInfo.setDownloadURL(registryHost + "/omtd-registry/request/corpus/download?archiveId=" + archiveID);

            distributionInfos.add(datasetDistributionInfo);
            corpusInfo.setDistributionInfos(distributionInfos);
            storeRESTClient.createSubArchive(archiveID, "metadata");
            storeRESTClient.createSubArchive(archiveID, "documents");
            storeRESTClient.createSubArchive(archiveID, "abstracts");
            corpusBuilderInfoDao.insert(metadataIdentifier.getValue(), queryString, CorpusStatus.SUBMITTED, archiveID);

            final String prepareMessage = "Prepare corpus Query: " + queryString;
            new Thread(()->producer.send(prepareMessage)).start();
            new Thread(()->producer.send("Prepare corpus CorpusID: " + metadataIdentifier.getValue())).start();
            new Thread(()->producer.send("Prepare corpus ArchiveID: " + archiveID)).start();
            new Thread(()->producer.send("Prepare corpus SubarchiveID: " + archiveID + "/metadata")).start();
            new Thread(()->producer.send("Prepare corpus SubarchiveID: " + archiveID + "/documents")).start();
            new Thread(()->producer.send("Prepare corpus SubarchiveID: " + archiveID + "/abstracts")).start();
        }

        return corpusMetadata;
    }

    /**
     * Method for building the corpus metadata.
     * Adds the corpusId to the execution queue
     * and the CorpusBuilderExecutionQueueConsumer
     * is responsible to create the new corpus
     * @param corpusMetadata the Corpus as built from the user
     */
    @Override
    public void buildCorpus(Corpus corpusMetadata) {

        if (contentConnectors != null) {
            corpora.add(corpusMetadata);
            new Thread(()->producer.send("Corpus Build: Sending corpus with id " + corpusMetadata.getMetadataHeaderInfo().getMetadataRecordIdentifier().getValue() + " to corpora queue for execution")).start();
        }
    }

    /**
     * Returns the status of the building process for a particular corpusId
     * @param s the corpusId
     * @return the status of the building process in the form of the CorpusStatus enum
     */
    @Override
    public CorpusStatus getStatus(String s) {

        CorpusBuilderInfoModel corpusBuilderInfoModel;

        try {
            corpusBuilderInfoModel = corpusBuilderInfoDao.find(s);
            new Thread(()->producer.send("Corpus " + s + " status: " + corpusBuilderInfoModel.getStatus())).start();
            return CorpusStatus.valueOf(corpusBuilderInfoModel.getStatus());
        } catch (EmptyResultDataAccessException e) {

            log.error("Corpus not found with corpusId " + s);
        } catch (Exception e) {

            log.error("CorpusBuilderImpl.getStatus", e);
        }
        return null;
    }

    /**
     * Cancels the building process for a particular corpusId
     * @param s the corpusId
     */
    @Override
    public void cancelProcess(String s) {

        try {
            CorpusBuilderInfoModel model = corpusBuilderInfoDao.find(s);

            if (model.getStatus().equalsIgnoreCase(CorpusStatus.SUBMITTED.toString())
                    || model.getStatus().equalsIgnoreCase(CorpusStatus.PROCESSING.toString())) {
                new Thread(() -> producer.send("Corpus " + s + " canceled")).start();
                corpusBuilderInfoDao.update(s, "status", CorpusStatus.CANCELED);
                storeRESTClient.deleteArchive(model.getArchiveId());
            }
        } catch (Exception e) {
            log.error("CorpusBuilderImpl.cancelProcess", e);
        }
    }

    /**
     * Deletes the corpus for a particular corpusId
     * @param s the corpusId
     */
    @Override
    public void deleteCorpus(String s) {

        try {
            CorpusBuilderInfoModel model = corpusBuilderInfoDao.find(s);
            corpusBuilderInfoDao.update(s, "status", CorpusStatus.DELETED);
            storeRESTClient.deleteArchive(model.getArchiveId());
            new Thread(()->producer.send("Corpus " + s + " deleted")).start();
            log.info(corpusBuilderInfoDao.find(s));
        } catch (Exception e) {
            log.error("CorpusBuilderImpl.deleteCorpus", e);
        }
    }

    /**
     * Method that creates the corpusId
     * @return a String with the new corpusId
     */
    private String createCorpusId() {
        return UUID.randomUUID().toString();
    }
}
