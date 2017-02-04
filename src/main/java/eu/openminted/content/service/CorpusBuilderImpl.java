package eu.openminted.content.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openminted.content.connector.*;
import eu.openminted.content.service.dao.CorpusBuilderInfoDao;
import eu.openminted.content.service.extensions.SearchResultExtension;
import eu.openminted.content.service.model.CorpusBuilderInfoModel;
import eu.openminted.content.service.tasks.FetchMetadataTask;
import eu.openminted.corpus.CorpusBuilder;
import eu.openminted.corpus.CorpusStatus;
import eu.openminted.registry.domain.*;
import eu.openminted.store.restclient.StoreRESTClient;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

@Component
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

    @org.springframework.beans.factory.annotation.Value("${tempDirectoryPath}")
    private String tempDirectoryPath;

    @org.springframework.beans.factory.annotation.Value("${store.host}")
    private String storeHost;

    @org.springframework.beans.factory.annotation.Value("${registry.host}")
    private String registryHost;

    @Override
    public Corpus prepareCorpus(Query query) {

        if (query == null) {
            query = new Query("*:*", new HashMap<>(), new ArrayList<>(), 0, 10);
        } else if (query.getKeyword() == null || query.getKeyword().isEmpty()) {
            query.setKeyword("*:*");
        }

        // if registryHost ends with '/' remove it
        registryHost = registryHost.replaceAll("/$", "");

        Corpus corpusMetadata = new Corpus();
        String queryString = "";
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
        metadataHeaderInfo.setMetadataLanguages(new ArrayList<>());
        corpusInfo.setDistributionInfos(new ArrayList<>());

        Facet sourceFacet = new Facet();
        SearchResultExtension result = new SearchResultExtension();
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
        String ancientGreek = "Greek, Ancient (to 1453)";
        String modernGreek = "Greek, Modern (1453-)";

        for (Facet facet : result.getFacets()) {
            // language
            if (facet.getField().equalsIgnoreCase("documentLanguage")) {
                for (Value value : facet.getValues()) {
                    if (value.getCount() > 0) {
                        Language language = new Language();
                        String name = value.getValue();
                        if (name.contains("Greek")) {
                            if (name.equalsIgnoreCase(ancientGreek)) {
                                name = ancientGreek;
                            } else {
                                name = modernGreek;
                            }
                        }
                        String code = LanguageConverter.getInstance().getLangNameToCode().get(name);
                        language.setLanguageTag(name);
                        language.setLanguageId(code);
                        metadataHeaderInfo.getMetadataLanguages().add(language);
                    }
                }
            }

            // licence
            if (facet.getField().equalsIgnoreCase("licence")) {
                for (Value value : facet.getValues()) {
                    if (value.getCount() > 0) {
                        DatasetDistributionInfo datasetDistributionInfo = new DatasetDistributionInfo();
                        RightsInfo rightsInfo = new RightsInfo();
                        rightsInfo.setLicenceInfos(new ArrayList<>());
                        LicenceInfo licenceInfo = new LicenceInfo();
                        licenceInfo.setLicence(LicenceEnum.NON_STANDARD_LICENCE_TERMS);
                        licenceInfo.setNonStandardLicenceTermsURL(value.getValue());

                        RightsStatementInfo rightsStatementInfo = new RightsStatementInfo();
                        rightsStatementInfo.setRightsStmtURL("http://api.openaire.eu/vocabularies/dnet:access_modes");
                        rightsStatementInfo.setRightsStmtName(RightsStmtNameConverter.convert(value.getValue()));

                        rightsInfo.getLicenceInfos().add(licenceInfo);
                        rightsInfo.setRightsStatementInfo(rightsStatementInfo);

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
            String archiveID = storeRESTClient.createArchive();
            DatasetDistributionInfo datasetDistributionInfo = new DatasetDistributionInfo();
            List<String> dowloadaURLs = new ArrayList<>();
            List<DatasetDistributionInfo> distributionInfos = new ArrayList<>();
            List<DistributionMediumEnum> distributionMediums = new ArrayList<>();

            dowloadaURLs.add(registryHost + "/omtd-registry/request/corpus/download?archiveId=" + archiveID);
            distributionMediums.add(DistributionMediumEnum.DOWNLOADABLE);

            datasetDistributionInfo.setDownloadURLs(dowloadaURLs);
            datasetDistributionInfo.setDistributionMediums(distributionMediums);

            distributionInfos.add(datasetDistributionInfo);
            corpusInfo.setDistributionInfos(distributionInfos);
            storeRESTClient.createSubArchive(archiveID, "metadata");
            storeRESTClient.createSubArchive(archiveID, "documents");
            corpusBuilderInfoDao.insert(metadataIdentifier.getValue(), queryString, CorpusStatus.SUBMITTED, archiveID);
        }

        return corpusMetadata;
    }

    @Override
    public void buildCorpus(Corpus corpusMetadata) {

        if (contentConnectors != null) {

            new Thread(() -> {
                CorpusBuilderInfoModel corpusBuilderInfoModel = null;
                try {
                    corpusBuilderInfoModel = corpusBuilderInfoDao.find(corpusMetadata.getMetadataHeaderInfo().getMetadataRecordIdentifier().getValue());

                    Collection<Future<?>> futures = new ArrayList<>();
                    Query query = new ObjectMapper().readValue(corpusBuilderInfoModel.getQuery(), Query.class);


                    for (ContentConnector connector : contentConnectors) {
                        FetchMetadataTask task = new FetchMetadataTask(storeRESTClient, connector, query, tempDirectoryPath, corpusBuilderInfoModel.getArchiveId());
                        futures.add(threadPoolExecutor.submit(task));
                    }
                    corpusBuilderInfoModel.setStatus(CorpusStatus.PROCESSING.toString());
                    corpusBuilderInfoDao.update(corpusBuilderInfoModel.getId(), "status", CorpusStatus.PROCESSING);
                    for (Future<?> future : futures) {
                        future.get();
                    }
                    corpusBuilderInfoModel.setStatus(CorpusStatus.CREATED.toString());
                    corpusBuilderInfoDao.update(corpusBuilderInfoModel.getId(), "status", CorpusStatus.CREATED);

                    //TODO: Email to user when corpus is ready which will include the landing page for the corpus
                } catch (Exception ex) {
                    log.error("CorpusBuilderImpl.buildCorpus", ex);
                    if (corpusBuilderInfoModel != null) {
                        corpusBuilderInfoModel.setStatus(CorpusStatus.CANCELED.toString());
                        corpusBuilderInfoDao.update(corpusBuilderInfoModel.getId(), "status", CorpusStatus.CANCELED);
                    }
                }
            }).start();
        }

        try {

            String url = registryHost + "/omtd-registry/request/corpus";
            RestTemplate restTemplate = new RestTemplate();
            restTemplate.postForObject(url, corpusMetadata, Corpus.class);
        } catch (HttpServerErrorException e) {
            log.error("CorpusBuilderImpl.buildCorpus: Error posting corpus at registry", e);
        }
    }

    @Override
    public CorpusStatus getStatus(String s) {

        CorpusBuilderInfoModel corpusBuilderInfoModel;

        try {
            corpusBuilderInfoModel = corpusBuilderInfoDao.find(s);
            return CorpusStatus.valueOf(corpusBuilderInfoModel.getStatus());
        } catch (Exception e) {
            log.error("CorpusBuilderImpl.cancelProcess", e);
        }
        return null;
    }

    @Override
    public void cancelProcess(String s) {

        try {
            corpusBuilderInfoDao.update(s, "status", CorpusStatus.CANCELED);
            log.info(corpusBuilderInfoDao.find(s));
        } catch (Exception e) {
            log.error("CorpusBuilderImpl.cancelProcess", e);
        }
    }

    @Override
    public void deleteCorpus(String s) {

        try {
            corpusBuilderInfoDao.update(s, "status", CorpusStatus.DELETED);
            log.info(corpusBuilderInfoDao.find(s));
        } catch (Exception e) {
            log.error("CorpusBuilderImpl.cancelProcess", e);
        }
    }

    private String createCorpusId() {
        return UUID.randomUUID().toString();
    }
}
