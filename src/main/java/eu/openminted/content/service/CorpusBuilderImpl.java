package eu.openminted.content.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.content.connector.utils.faceting.OMTDFacetEnum;
import eu.openminted.content.connector.utils.faceting.OMTDFacetLabels;
import eu.openminted.content.connector.utils.language.LanguageUtils;
import eu.openminted.content.service.database.CorpusBuilderInfoDao;
import eu.openminted.content.service.messages.JMSConsumer;
import eu.openminted.content.service.messages.JMSProducer;
import eu.openminted.content.service.model.CorpusBuilderInfoModel;
import eu.openminted.content.service.process.CorpusBuilderExecutionQueueConsumer;
import eu.openminted.corpus.CorpusBuilder;
import eu.openminted.corpus.CorpusBuildingState;
import eu.openminted.corpus.CorpusStatus;
import eu.openminted.registry.core.domain.Facet;
import eu.openminted.registry.core.domain.Value;
import eu.openminted.registry.domain.*;
import eu.openminted.store.restclient.StoreRESTClient;
import org.apache.log4j.Logger;
import org.mitre.openid.connect.model.OIDCAuthenticationToken;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import static eu.openminted.content.connector.utils.SearchExtensions.merge;

@Component
@ComponentScan("eu.openminted.content")
public class CorpusBuilderImpl implements CorpusBuilder {
    private static Logger log = Logger.getLogger(CorpusBuilderImpl.class.getName());

    @Autowired
    private OMTDFacetLabels omtdFacetInitializer;

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
    private LanguageUtils languageUtils;

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

        new Thread(() -> consumer.listen()).start();
    }

    /**
     * Method for preparing the corpus building process for a new corpus
     *
     * @param query the query as inserted in the prepare method of the controller
     * @return the Coprus that the user is building
     */
    @Override
    public Corpus prepareCorpus(Query query) {

        // if registryHost ends with '/' remove it
        registryHost = registryHost.replaceAll("/$", "");

        Corpus corpusMetadata = new Corpus();
        String queryString = "";
        String descriptionString = "A corpus generated automatically by [user_name] " +
                "on [creation_date] via OpenMinTeD services. " +
                "The corpus includes [number_of] publications from [source] in [language]";

        String resourceNameUsageDescription = "OpenMinTeD subset of [connectors] publications";
        StringBuilder sourcesBuilder = new StringBuilder();

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
        sourceFacet.setField(OMTDFacetEnum.SOURCE.value());
        sourceFacet.setLabel(omtdFacetInitializer.getOmtdFacetLabels().get(OMTDFacetEnum.SOURCE));
        sourceFacet.setValues(new ArrayList<>());

        // retrieve connectors from query
        List<String> connectors = new ArrayList<>();
        if (query.getParams().containsKey(OMTDFacetEnum.SOURCE.value())
                && query.getParams().get(OMTDFacetEnum.SOURCE.value()) != null
                && query.getParams().get(OMTDFacetEnum.SOURCE.value()).size() > 0) {
            connectors.addAll(query.getParams().get(OMTDFacetEnum.SOURCE.value()));
        }

        if (contentConnectors != null) {

            if (!tempQuery.getFacets().contains(OMTDFacetEnum.PUBLICATION_TYPE.value()))
                tempQuery.getFacets().add(OMTDFacetEnum.PUBLICATION_TYPE.value());
            if (!tempQuery.getFacets().contains(OMTDFacetEnum.PUBLICATION_YEAR.value()))
                tempQuery.getFacets().add(OMTDFacetEnum.PUBLICATION_YEAR.value());
            if (!tempQuery.getFacets().contains(OMTDFacetEnum.RIGHTS.value()))
                tempQuery.getFacets().add(OMTDFacetEnum.RIGHTS.value());
            if (!tempQuery.getFacets().contains(OMTDFacetEnum.DOCUMENT_LANG.value()))
                tempQuery.getFacets().add(OMTDFacetEnum.DOCUMENT_LANG.value());

            for (ContentConnector connector : contentConnectors) {
                if (connectors.size() > 0 && !connectors.contains(connector.getSourceName())) continue;

                SearchResult res = connector.search(tempQuery);
                Value value = new Value();

                value.setValue(connector.getSourceName());
                value.setCount(res.getTotalHits());

                sourceFacet.getValues().add(value);

                merge(result, res);

                // Remove values with count 0 and set labels to facets
                result.getFacets().forEach(facet-> {
                    List<Value> valuesToRemove = new ArrayList<>();
                    facet.getValues().forEach(value1 -> {

                        if (value1.getCount() == 0) valuesToRemove.add(value1);
                    });

                    if (valuesToRemove.size() > 0) {
                        facet.getValues().removeAll(valuesToRemove);
                    }

                    OMTDFacetEnum facetEnum = OMTDFacetEnum.fromValue(facet.getField());
                    if (facetEnum != null)
                        facet.setLabel(omtdFacetInitializer.getOmtdFacetLabels().get(facetEnum));
                });

                sourcesBuilder.append(connector.getSourceName()).append(" and ");
            }
            sourcesBuilder = sourcesBuilder.delete(sourcesBuilder.lastIndexOf(" and "), sourcesBuilder.length());

            String connectorsToString = contentConnectors.stream()
                    .map(ContentConnector::getSourceName)
                    .collect(Collectors.toList()).toString()
                    .replaceAll("\\[|\\]", "");

            resourceNameUsageDescription = resourceNameUsageDescription.replaceAll("\\[connectors\\]", connectorsToString);
            ResourceName resourceName = new ResourceName();
            resourceName.setLang("en");
            resourceName.setValue(resourceNameUsageDescription);
            identificationInfo.getResourceNames().add(resourceName);
            corpusInfo.setIdentificationInfo(identificationInfo);
        }

        result.getFacets().add(sourceFacet);

        for (Facet facet : result.getFacets()) {
            // language
            if (facet.getField().equalsIgnoreCase(OMTDFacetEnum.DOCUMENT_LANG.value())) {
                CorpusTextPartInfo corpusTextPartInfo = new CorpusTextPartInfo();
                CorpusMediaPartsType corpusMediaPartsType = new CorpusMediaPartsType();
                RawCorpusInfo rawCorpusInfo = new RawCorpusInfo();
                CorpusSubtypeSpecificInfo corpusSubtypeSpecificInfo = new CorpusSubtypeSpecificInfo();

                rawCorpusInfo.setCorpusMediaPartsType(corpusMediaPartsType);
                corpusSubtypeSpecificInfo.setRawCorpusInfo(rawCorpusInfo);
                corpusInfo.setCorpusSubtypeSpecificInfo(corpusSubtypeSpecificInfo);

                corpusInfo.getCorpusSubtypeSpecificInfo().getRawCorpusInfo().getCorpusMediaPartsType().getCorpusTextParts().add(corpusTextPartInfo);

                Description description = new Description();
                description.setLang("en");
                String currentDescription = descriptionString;
                int publicationsCounter = 0;
                int languagesCounter = 0;
                currentDescription = currentDescription.replaceAll("\\[creation_date\\]", new java.util.Date().toString());
                currentDescription = currentDescription.replaceAll("\\[source\\]", sourcesBuilder.toString());

                for (Value value : facet.getValues()) {
                    if (value.getCount() > 0) {
                        languagesCounter++;
                        publicationsCounter += value.getCount();

                        Language language = new Language();
                        LanguageInfo languageInfo = new LanguageInfo();

                        String name = value.getValue();
                        String code = languageUtils.getLangNameToCode().get(name);
                        language.setLanguageTag(name);
                        language.setLanguageId(code);
                        languageInfo.setLanguage(language);
                        SizeInfo languageSizeInfo = new SizeInfo();
                        languageSizeInfo.setSize(String.valueOf(value.getCount()));
                        languageSizeInfo.setSizeUnit(SizeUnitEnum.TEXTS);
                        languageInfo.setSizePerLanguage(languageSizeInfo);

                        corpusTextPartInfo.getLanguages().add(languageInfo);
                    }
                }

                currentDescription = currentDescription.replaceAll("\\[number_of\\]", "" + publicationsCounter);
                currentDescription = currentDescription.replaceAll("\\[language\\]", languagesCounter + " languages");

                description.setValue(currentDescription);
                corpusInfo.getIdentificationInfo().getDescriptions().add(description);

                LingualityInfo lingualityInfo = new LingualityInfo();

                if (languagesCounter == 1) {
                    lingualityInfo.setLingualityType(LingualityTypeEnum.MONOLINGUAL);
                } else if (languagesCounter == 2) {
                    lingualityInfo.setLingualityType(LingualityTypeEnum.BILINGUAL);
                } else if (languagesCounter > 2) {
                    lingualityInfo.setLingualityType(LingualityTypeEnum.MULTILINGUAL);
                } else {
                    lingualityInfo.setLingualityType(LingualityTypeEnum.MONOLINGUAL);
                }

                corpusTextPartInfo.setLingualityInfo(lingualityInfo);

                SizeInfo sizeInfo = new SizeInfo();
                sizeInfo.setSize(String.valueOf(result.getTotalHits()));
                sizeInfo.setSizeUnit(SizeUnitEnum.TEXTS);
                corpusTextPartInfo.getSizes().add(sizeInfo);
            }

            // licence
            if (facet.getField().equalsIgnoreCase(OMTDFacetEnum.RIGHTS.value())) {
                for (Value value : facet.getValues()) {
                    if (value.getCount() > 0) {
                        DatasetDistributionInfo datasetDistributionInfo = new DatasetDistributionInfo();
                        RightsInfo rightsInfo = createRightsInfo(value);
                        datasetDistributionInfo.setRightsInfo(rightsInfo);
                        SizeInfo sizeInfo = createSizeInfo(value);
                        datasetDistributionInfo.getSizes().add(sizeInfo);
                        corpusInfo.getDistributionInfos().add(datasetDistributionInfo);
                    }
                }
            }
        }

        ResourceIdentifier resourceIdentifier = new ResourceIdentifier();
        resourceIdentifier.setValue(UUID.randomUUID().toString());
        identificationInfo.getResourceIdentifiers().add(resourceIdentifier);

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

            DistributionLoc distributionLoc = new DistributionLoc();
            distributionLoc.setDistributionMedium(DistributionMediumEnum.DOWNLOADABLE);

            distributionLoc.setDistributionLocation(registryHost + "/request/corpus/download?archiveId=" + archiveID);
            datasetDistributionInfo.getDistributionLoc().add(distributionLoc);

            distributionInfos.add(datasetDistributionInfo);
            corpusInfo.setDistributionInfos(distributionInfos);
            storeRESTClient.createSubArchive(archiveID, "metadata");
            storeRESTClient.createSubArchive(archiveID, "fulltext");
            storeRESTClient.createSubArchive(archiveID, "abstract");

            try {
                OIDCAuthenticationToken authentication = (OIDCAuthenticationToken) SecurityContextHolder.getContext().getAuthentication();
                corpusBuilderInfoDao.insert(metadataIdentifier.getValue(), authentication.getSub(), queryString, CorpusStatus.INITIATING, archiveID);
            } catch (ClassCastException e) {
                log.error("User is not authenticated to build corpus", e);
            }
            final String prepareMessage = "Prepare corpus Query: " + queryString;
            new Thread(() -> producer.sendMessage("String", prepareMessage)).start();
            new Thread(() -> producer.sendMessage("String", "Prepare corpus CorpusID: " + metadataIdentifier.getValue())).start();
            new Thread(() -> producer.sendMessage("String", "Prepare corpus ArchiveID: " + archiveID)).start();
            new Thread(() -> producer.sendMessage("String", "Prepare corpus SubarchiveID: " + archiveID + "/metadata")).start();
            new Thread(() -> producer.sendMessage("String", "Prepare corpus SubarchiveID: " + archiveID + "/fulltext")).start();
            new Thread(() -> producer.sendMessage("String", "Prepare corpus SubarchiveID: " + archiveID + "/abstract")).start();
        }

        return corpusMetadata;
    }

    /**
     * Method for building the corpus metadata.
     * Adds the corpusId to the execution queuere
     * and the CorpusBuilderExecutionQueueConsumer
     * is responsible to create the new corpus
     *
     * @param corpusMetadata the Corpus as built from the user
     */
    @Override
    public void buildCorpus(Corpus corpusMetadata) {
        String authenticationSub = "";
        try {
            OIDCAuthenticationToken authentication = (OIDCAuthenticationToken) SecurityContextHolder.getContext().getAuthentication();
            authenticationSub = authentication.getSub();
            if (authenticationSub != null || !authenticationSub.isEmpty()) {
                String corpusId = corpusMetadata.getMetadataHeaderInfo().getMetadataRecordIdentifier().getValue();
                if (contentConnectors != null) {
                    corpora.add(corpusMetadata);
                    corpusBuilderInfoDao.updateStatus(corpusId, CorpusStatus.SUBMITTED);
                    CorpusBuilderInfoModel corpusBuilderInfoModel = corpusBuilderInfoDao.find(corpusId);
                    Query query = new ObjectMapper().readValue(corpusBuilderInfoModel.getQuery(), Query.class);

                    for (ContentConnector connector : contentConnectors) {
                        SearchResult searchResult = connector.search(query);
                        if (searchResult.getTotalHits() == 0)
                            continue;

                        CorpusBuildingState corpusBuildingState = new CorpusBuildingState();
                        corpusBuildingState.setId(corpusId + "@" + connector.getSourceName());
                        corpusBuildingState.setToken(authenticationSub);
                        corpusBuildingState.setCurrentStatus(CorpusStatus.SUBMITTED.toString());
                        corpusBuildingState.setConnector(connector.getSourceName());
                        corpusBuildingState.setTotalHits(corpusMetadata.getCorpusInfo().getCorpusSubtypeSpecificInfo().getRawCorpusInfo().getCorpusMediaPartsType().getCorpusTextParts().size());
                        new Thread(() -> producer.sendMessage(CorpusBuildingState.class.toString(), corpusBuildingState)).start();
                    }
                }
            } else {
                log.error("User is not authenticated to build corpus");
            }

        } catch (ClassCastException e) {
            log.error("User is not authenticated to build corpus", e);
        } catch (IOException e) {
            log.error("IO error while initiating corpus building", e);
        }
    }

    /**
     * Returns the status of the building process for a particular corpusId
     *
     * @param s the corpusId
     * @return the status of the building process in the form of the CorpusStatus enum
     */
    @Override
    public CorpusStatus getStatus(String s) {

        CorpusBuilderInfoModel corpusBuilderInfoModel;

        try {
            corpusBuilderInfoModel = corpusBuilderInfoDao.find(s);
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
     *
     * @param s the corpusId
     */
    @Override
    public void cancelProcess(String s) {

        try {
            CorpusBuilderInfoModel model = corpusBuilderInfoDao.find(s);

            if (model.getStatus().equalsIgnoreCase(CorpusStatus.SUBMITTED.toString())
                    || model.getStatus().equalsIgnoreCase(CorpusStatus.PROCESSING.toString())) {
                corpusBuilderInfoDao.update(s, "status", CorpusStatus.CANCELED);
                storeRESTClient.deleteArchive(model.getArchiveId());
            }
        } catch (Exception e) {
            log.error("CorpusBuilderImpl.cancelProcess", e);
        }
    }

    /**
     * Deletes the corpus for a particular corpusId
     *
     * @param s the corpusId
     */
    @Override
    public void deleteCorpus(String s) {

        try {
            CorpusBuilderInfoModel model = corpusBuilderInfoDao.find(s);
            corpusBuilderInfoDao.update(s, "status", CorpusStatus.DELETED);
            storeRESTClient.deleteArchive(model.getArchiveId());
            log.info(corpusBuilderInfoDao.find(s));
        } catch (Exception e) {
            log.error(e);
        }
    }

    /**
     * Method that creates the corpusId
     *
     * @return a String with the new corpusId
     */
    private String createCorpusId() {
        return UUID.randomUUID().toString();
    }

    private RightsInfo createRightsInfo(Value value) {
        RightsInfo rightsInfo = new RightsInfo();
        LicenceInfo licenceInfo = new LicenceInfo();
        licenceInfo.setLicence(LicenceEnum.NON_STANDARD_LICENCE_TERMS);
        licenceInfo.setNonStandardLicenceTermsURL(value.getValue());

        LicenceInfos licenceInfos = new LicenceInfos();
        licenceInfos.getLicenceInfo().add(licenceInfo);
        rightsInfo.setLicenceInfos(new ArrayList<>());
        rightsInfo.getLicenceInfos().add(licenceInfos);
        rightsInfo.setRightsStatement(new ArrayList<>());
        rightsInfo.getRightsStatement().add(omtdFacetInitializer.getOmtdGetRightsStmtEnumFromLabel().get(value.getValue()));
        return rightsInfo;
    }

    private SizeInfo createSizeInfo(Value value) {
        SizeInfo sizeInfo = new SizeInfo();
        sizeInfo.setSize(String.valueOf(value.getCount()));
        sizeInfo.setSizeUnit(SizeUnitEnum.TEXTS);
        return sizeInfo;
    }
}
