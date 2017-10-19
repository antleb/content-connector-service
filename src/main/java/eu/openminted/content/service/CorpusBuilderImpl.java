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
    private OMTDFacetLabels omtdFacetLabels;

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

    @org.springframework.beans.factory.annotation.Value("${authentication.token.name:omtd-user}")
    private String tokenName;

    @org.springframework.beans.factory.annotation.Value("${authentication.token.email:openminted@gmail.com}")
    private String tokenEmail;

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

        log.info("Start looking for models in database");
        List<CorpusBuilderInfoModel> corpusBuilderInfoModels = corpusBuilderInfoDao.findAllUnfinished();
        if (corpusBuilderInfoModels != null && corpusBuilderInfoModels.size() > 0) {
            for (CorpusBuilderInfoModel model : corpusBuilderInfoModels) {
                try {
                    log.info(model);
                    buildCorpusPerModel(model);
                } catch (Exception e) {
                    log.error("Error updating corpora. Skipping model " + model.toString(), e);
                }
            }
        }
    }

    private void buildCorpusPerModel(CorpusBuilderInfoModel model) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Corpus corpus = mapper.readValue(model.getCorpus(), Corpus.class);
        buildCorpus(corpus, model.getToken());
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
        MetadataIdentifier metadataIdentifier = createMetadataIdentifier(metadataHeaderInfo);

        metadataHeaderInfo.setUserQuery(tempQuery.getKeyword());
        corpusInfo.setDistributionInfos(new ArrayList<>());

        // prepare facets
        SearchResult result = new SearchResult();
        result.setFacets(new ArrayList<>());
        Facet sourceFacet = createSourceFacet();

        // retrieve connectors from query
        List<String> connectors = createListOfConnectors(query);

        if (contentConnectors != null) {
            updateFacets(tempQuery);

            for (ContentConnector connector : contentConnectors) {
                if (connectors.size() > 0 && !connectors.contains(connector.getSourceName())) continue;

                SearchResult res = connector.search(tempQuery);
                Value value = new Value();

                value.setValue(connector.getSourceName());
                value.setCount(res.getTotalHits());

                sourceFacet.getValues().add(value);

                merge(result, res);

                // Remove values with count 0 and set labels to facets
                result.getFacets().forEach(facet -> {
                    List<Value> valuesToRemove = new ArrayList<>();
                    facet.getValues().forEach(value1 -> {
                        if (value1.getCount() == 0) valuesToRemove.add(value1);
                    });

                    if (valuesToRemove.size() > 0) {
                        facet.getValues().removeAll(valuesToRemove);
                    }

                    OMTDFacetEnum facetEnum = OMTDFacetEnum.fromValue(facet.getField());
                    if (facetEnum != null)
                        facet.setLabel(omtdFacetLabels.getFacetLabelsFromEnum(facetEnum));
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
                addCorpusLanguageFields(facet, corpusInfo, descriptionString, sourcesBuilder, result);
            }

            // licence
            if (facet.getField().equalsIgnoreCase(OMTDFacetEnum.RIGHTS.value())) {
                addCorpusLicenceFields(facet, corpusInfo);
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

            distributionLoc.setDistributionLocation(registryHost + "/api/request/corpus/download?archiveId=" + archiveID);
            datasetDistributionInfo.getDistributionLoc().add(distributionLoc);

            distributionInfos.add(datasetDistributionInfo);
            corpusInfo.setDistributionInfos(distributionInfos);
            storeRESTClient.createSubArchive(archiveID, "metadata");
            storeRESTClient.createSubArchive(archiveID, "fulltext");
            storeRESTClient.createSubArchive(archiveID, "abstract");

            try {
                OIDCAuthenticationToken authentication = (OIDCAuthenticationToken) SecurityContextHolder.getContext().getAuthentication();
                populateCorpus(corpusMetadata);
                ObjectMapper objectMapper = new ObjectMapper();
                String corpus = objectMapper.writeValueAsString(corpusMetadata);
                log.info(corpus);
                corpusBuilderInfoDao.insert(metadataIdentifier.getValue(), authentication.getSub(), queryString, corpus, CorpusStatus.INITIATING, archiveID);
            } catch (ClassCastException e) {
                log.error("User is not authenticated to build corpus", e);
            } catch (JsonProcessingException e) {
                log.error("Corpus metadata cannot be written as string", e);
            }
        }

        return corpusMetadata;
    }

    private void addCorpusLanguageFields(Facet facet,
                                         CorpusInfo corpusInfo,
                                         String descriptionString,
                                         StringBuilder sourcesBuilder,
                                         SearchResult result) {
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

                language.setLanguageTag(value.getLabel());
                language.setLanguageId(value.getValue());
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

    private void addCorpusLicenceFields(Facet facet, CorpusInfo corpusInfo) {
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

    private void updateFacets(Query tempQuery) {
        if (!tempQuery.getFacets().contains(OMTDFacetEnum.PUBLICATION_TYPE.value()))
            tempQuery.getFacets().add(OMTDFacetEnum.PUBLICATION_TYPE.value());
        if (!tempQuery.getFacets().contains(OMTDFacetEnum.PUBLICATION_YEAR.value()))
            tempQuery.getFacets().add(OMTDFacetEnum.PUBLICATION_YEAR.value());
        if (!tempQuery.getFacets().contains(OMTDFacetEnum.RIGHTS.value()))
            tempQuery.getFacets().add(OMTDFacetEnum.RIGHTS.value());
        if (!tempQuery.getFacets().contains(OMTDFacetEnum.DOCUMENT_LANG.value()))
            tempQuery.getFacets().add(OMTDFacetEnum.DOCUMENT_LANG.value());
        if (!tempQuery.getFacets().contains(OMTDFacetEnum.DOCUMENT_TYPE.value()))
            tempQuery.getFacets().add(OMTDFacetEnum.DOCUMENT_TYPE.value());
    }

    private List<String> createListOfConnectors(Query query) {
        List<String> connectors = new ArrayList<>();
        if (query.getParams().containsKey(OMTDFacetEnum.SOURCE.value())
                && query.getParams().get(OMTDFacetEnum.SOURCE.value()) != null
                && query.getParams().get(OMTDFacetEnum.SOURCE.value()).size() > 0) {
            connectors.addAll(query.getParams().get(OMTDFacetEnum.SOURCE.value()));
        }
        return connectors;
    }

    private Facet createSourceFacet() {
        Facet sourceFacet = new Facet();
        sourceFacet.setField(OMTDFacetEnum.SOURCE.value());
        sourceFacet.setLabel(omtdFacetLabels.getFacetLabelsFromEnum(OMTDFacetEnum.SOURCE));
        sourceFacet.setValues(new ArrayList<>());
        return sourceFacet;
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
        String authenticationSub;
        try {
            OIDCAuthenticationToken authentication = (OIDCAuthenticationToken) SecurityContextHolder.getContext().getAuthentication();
            authenticationSub = authentication.getSub();
            if (authenticationSub != null || !authenticationSub.isEmpty()) {
                startBuildingCorpus(corpusMetadata, authenticationSub);
            } else {
                log.error("User is not authenticated to build corpus");
            }

        } catch (ClassCastException e) {
            log.error("User is not authenticated to build corpus", e);
        } catch (IOException e) {
            log.error("IO error while initiating corpus building", e);
        }
    }


    private void buildCorpus(Corpus corpusMetadata, String authenticationSub) {
        try {
            if (authenticationSub != null || !authenticationSub.isEmpty()) {
                startBuildingCorpus(corpusMetadata, authenticationSub);
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
        rightsInfo.getRightsStatement().add(omtdFacetLabels.getRightsStmtEnumFromLabel().get(value.getValue()));
        return rightsInfo;
    }

    private SizeInfo createSizeInfo(Value value) {
        SizeInfo sizeInfo = new SizeInfo();
        sizeInfo.setSize(String.valueOf(value.getCount()));
        sizeInfo.setSizeUnit(SizeUnitEnum.TEXTS);
        return sizeInfo;
    }

    private void startBuildingCorpus(Corpus corpusMetadata, String authenticationSub) throws IOException {
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
                producer.sendMessage(corpusBuildingState);
            }
        }
    }

    private void populateCorpus(Corpus corpus) {
        String username = "";
        ContactInfo contactInfo = new ContactInfo();
        List<Name> names = new ArrayList<>();
        List<String> emails = new ArrayList<>();
        List<PersonInfo> personInfos = new ArrayList<>();
        CommunicationInfo communicationInfo = new CommunicationInfo();

        PersonInfo personInfo = new PersonInfo();

        if (SecurityContextHolder.getContext() != null && SecurityContextHolder.getContext().getAuthentication() instanceof OIDCAuthenticationToken) {
            OIDCAuthenticationToken authentication = (OIDCAuthenticationToken) SecurityContextHolder.getContext().getAuthentication();
            getNamesFromAuthenticationToken(authentication, names);
            personInfo.setNames(names);

            emails.add(authentication.getUserInfo().getEmail());
            communicationInfo.setEmails(emails);
            personInfo.setCommunicationInfo(communicationInfo);
            personInfos.add(personInfo);

            contactInfo.setContactEmail(authentication.getUserInfo().getEmail());
            contactInfo.setContactPersons(personInfos);

            username = authentication.getUserInfo().getName();
            if (username == null) username = "";
        } else {
            log.warn("There is no valid authentication token. Going with default email.");
            Name name = new Name();
            name.setValue(tokenName);
            name.setLang("en");

            names.add(name);
            personInfo.setNames(names);
            personInfos.add(personInfo);

            contactInfo.setContactEmail(tokenEmail);
            contactInfo.setContactPersons(personInfos);

            username = tokenName;
        }
        corpus.getCorpusInfo().setContactInfo(contactInfo);
        for (Description description : corpus.getCorpusInfo().getIdentificationInfo().getDescriptions()) {
            if (username != null || !username.isEmpty())
                description.setValue(description.getValue().replaceAll("\\[user_name\\]", username));
        }
    }

    private void getNamesFromAuthenticationToken(OIDCAuthenticationToken authentication, List<Name> names) {
        Name name = new Name();
        Name familyName = new Name();
        Name givenName = new Name();
        Name preferredUsername = new Name();

        name.setValue(authentication.getUserInfo().getName());
        familyName.setValue(authentication.getUserInfo().getFamilyName());
        givenName.setValue(authentication.getUserInfo().getGivenName());
        preferredUsername.setValue(authentication.getUserInfo().getPreferredUsername());

        name.setLang("en");
        familyName.setLang("en");
        givenName.setLang("en");
        preferredUsername.setLang("en");

        names.add(name);
        names.add(familyName);
        names.add(givenName);
        names.add(preferredUsername);
    }

    private MetadataIdentifier createMetadataIdentifier(MetadataHeaderInfo metadataHeaderInfo) {
        MetadataIdentifier metadataIdentifier = new MetadataIdentifier();

        metadataIdentifier.setValue(createCorpusId());
        metadataIdentifier.setMetadataIdentifierSchemeName(MetadataIdentifierSchemeNameEnum.OTHER);
        metadataHeaderInfo.setMetadataRecordIdentifier(metadataIdentifier);
        return metadataIdentifier;
    }
}
