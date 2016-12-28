package eu.openminted.content.service;

import eu.openminted.content.connector.*;
import eu.openminted.content.service.dao.CorpusDao;
import eu.openminted.content.service.extensions.SearchResultExtension;
import eu.openminted.corpus.CorpusBuilder;
import eu.openminted.corpus.CorpusStatus;
import eu.openminted.registry.domain.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by constantine on 22/12/2016.
 */
@Component
public class CorpusBuilderImpl implements CorpusBuilder {

    @Autowired(required = false)
    private List<ContentConnector> contentConnectors;

    @Autowired
    private CorpusDao corpusDao;

    @Override
    public Corpus prepareCorpus(Query query) {
        Corpus corpusMetadata = new Corpus();

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

        metadataHeaderInfo.setUserQuery(query.getKeyword());
        metadataHeaderInfo.setMetadataLanguages(new ArrayList<>());
        corpusInfo.setDistributionInfos(new ArrayList<>());

        Facet sourceFacet = new Facet();
        SearchResultExtension result = new SearchResultExtension();
        result.setFacets(new ArrayList<>());
        sourceFacet.setField("source");
        sourceFacet.setLabel("Content Source");
        sourceFacet.setValues(new ArrayList<>());

        if (contentConnectors != null) {
            query.setFrom(0);
            query.setTo(1);
            if (query.getFacets() == null)
                query.setFacets(new ArrayList<>());

            if (!query.getFacets().contains("licence"))
                query.getFacets().add("licence");
            if (!query.getFacets().contains("documentType"))
                query.getFacets().add("documentType");
            if (!query.getFacets().contains("documentLanguage"))
                query.getFacets().add("documentLanguage");

            for (ContentConnector connector : contentConnectors) {
                SearchResult res = connector.search(query);
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
            if(facet.getField().equalsIgnoreCase("documentLanguage")) {
                for (Value value : facet.getValues()) {
                    if (value.getCount() > 0) {
                        Language language = new Language();
                        String name = value.getValue();
                        if (name.contains("Greek")) {
                            if (name.equalsIgnoreCase(ancientGreek)) {
                                name = ancientGreek;
                            }
                            else {
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
            if(facet.getField().equalsIgnoreCase("licence")) {
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
        corpusDao.insert(metadataIdentifier.getValue(), query.toString());
        System.out.println(corpusDao.findAll());
        return corpusMetadata;
    }

    @Override
    public void buildCorpus(Corpus corpusMetadata) {

    }

    @Override
    public CorpusStatus getStatus(String s) {
        return null;
    }

    @Override
    public void cancelProcess(String s) {

    }

    @Override
    public void deleteCorpus(String s) {

    }

    private String createCorpusId() {
        return UUID.randomUUID().toString();
    }
}
