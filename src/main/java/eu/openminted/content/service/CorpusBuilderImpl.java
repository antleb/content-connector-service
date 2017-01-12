package eu.openminted.content.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openminted.content.connector.*;
import eu.openminted.content.service.dao.CorpusBuilderInfoDao;
import eu.openminted.content.service.extensions.FetchMetadataTask;
import eu.openminted.content.service.extensions.SearchResultExtension;
import eu.openminted.content.service.model.CorpusBuilderInfoModel;
import eu.openminted.corpus.CorpusBuilder;
import eu.openminted.corpus.CorpusStatus;
import eu.openminted.registry.domain.*;
import eu.openminted.store.restclient.StoreRESTClient;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
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

    @Override
    public Corpus prepareCorpus(Query query) {
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
            System.out.println(facet.getField());

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
                    System.out.println(value.getValue() + " : " + value.getCount());
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
            log.info(queryString);
        } catch (JsonProcessingException e) {
            log.error("CorpusBuilderImpl.prepareCorpus: Unable to write value as String", e);
        }

        /*
            Store service
            open archive with corpusId
            open subarchive metadata with name corpusId_metadata
            open subarchive fulltext with name corpusId_fulltext
         */

        String archiveID = storeRESTClient.createArchive();

        if (!queryString.isEmpty())
            corpusBuilderInfoDao.insert(metadataIdentifier.getValue(), queryString, CorpusStatus.CREATED, archiveID);

        return corpusMetadata;
    }

    @Override
    public void buildCorpus(Corpus corpusMetadata) {
        try {
            CorpusBuilderInfoModel corpusBuilderInfoModel = corpusBuilderInfoDao.find(corpusMetadata.getMetadataHeaderInfo().getMetadataRecordIdentifier().getValue());
            System.out.println(corpusBuilderInfoModel);
            Query query = new ObjectMapper().readValue(corpusBuilderInfoModel.getQuery(), Query.class);
            corpusBuilderInfoModel.setStatus(CorpusStatus.SUBMITTED.toString());
            File archive = new File("/Users/constantine/" + corpusBuilderInfoModel.getArchiveId());
            archive.mkdirs();

            if (contentConnectors != null) {
//                threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(contentConnectors.size());
                for (ContentConnector connector : contentConnectors) {

                    FetchMetadataTask task = new FetchMetadataTask(storeRESTClient, connector, query, corpusBuilderInfoModel.getArchiveId());
                    log.info("A new task has been added : " + connector.getSourceName());
                    threadPoolExecutor.execute(task);
                }

                System.out.println("Maximum threads inside pool " + threadPoolExecutor.getMaximumPoolSize());

                corpusBuilderInfoDao.update(corpusBuilderInfoModel.getId(), "status", CorpusStatus.SUBMITTED);
                corpusBuilderInfoModel = corpusBuilderInfoDao.find(corpusBuilderInfoModel.getId());
                System.out.println(corpusBuilderInfoModel);
            }
        } catch (Exception ex) {
            log.error("CorpusBuilderImpl.buildCorpus", ex);
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
