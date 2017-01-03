package eu.openminted.content.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openminted.content.connector.*;
import eu.openminted.content.service.dao.CorpusDao;
import eu.openminted.content.service.extensions.SearchResultExtension;
import eu.openminted.content.service.model.CorpusModel;
import eu.openminted.corpus.CorpusBuilder;
import eu.openminted.corpus.CorpusStatus;
import eu.openminted.registry.domain.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

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
        String queryString;
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
        }
        catch (JsonProcessingException e) {
            e.printStackTrace();
            queryString = "{\"keyword\":\"" + query.getKeyword() + "\",\"params\":{";
            String parameter = "";
            for (String paramKey : query.getParams().keySet()) {
                if (!parameter.isEmpty()) {
                    queryString += ",";
                }
                parameter = '\"' + paramKey + "\":[\"" + String.join("\",\"", query.getParams().get(paramKey)) + "\"]";
                queryString += parameter;
            }
            queryString += "},\"facets\":[\"" +
                    String.join("\",\"", query.getFacets()) +
                    "\"],\"from\":" +
                    query.getFrom() +
                    ",\"to\":" +
                    query.getTo();
        }

        corpusDao.insert(metadataIdentifier.getValue(), queryString);
        return corpusMetadata;
    }

    @Override
    public void buildCorpus(Corpus corpusMetadata) {
        try {
            CorpusModel corpusModel = corpusDao.find(corpusMetadata.getMetadataHeaderInfo().getMetadataRecordIdentifier().getValue());

            System.out.println(corpusModel.getQuery());
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
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
