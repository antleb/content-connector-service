package eu.openminted.content.service.rest;

import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.content.service.ContentService;
import eu.openminted.content.service.dao.CorpusBuilderInfoDao;
import eu.openminted.corpus.CorpusBuilder;
import eu.openminted.corpus.CorpusStatus;
import eu.openminted.registry.domain.ContactInfo;
import eu.openminted.registry.domain.Corpus;
import eu.openminted.registry.domain.MetadataHeaderInfo;
import eu.openminted.registry.domain.MetadataIdentifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Value;

import java.util.*;

@RestController
public class ContentServiceController {
    @Autowired
    private CorpusBuilderInfoDao corpusBuilderInfoDao;

    @Autowired
    private ContentService contentService;

    @Autowired
    private CorpusBuilder corpusBuilder;

    @Value("${authentication.token.email}")
    private String tokenEmail;

    @RequestMapping(value = "/content/browse", method = RequestMethod.GET, headers = "Accept=application/json")
    public SearchResult browse(@RequestParam(value = "facets") String facets) {

        List<String> facetList = Arrays.asList(facets.split(","));
        return contentService.search(new Query("", new HashMap<>(), facetList, 0, 10));
    }

    @RequestMapping(value = "/content/browse", method = RequestMethod.POST, headers = "Accept=application/json")
    public SearchResult browse(@RequestBody Query query) {

        if (query == null) throw new NullPointerException("Query should not be null");
        return contentService.search(query);
    }

    @RequestMapping(value = "/corpus/prepare", method = RequestMethod.GET, headers = "Accept=application/json")
    public Corpus prepare(@RequestParam(value = "facets") String facets) {

        List<String> facetList = new ArrayList<>(Arrays.asList(facets.split(",")));
        Query query = new Query("*:*", new HashMap<>(), facetList, 0, 1);
        return corpusBuilder.prepareCorpus(query);
    }

    @RequestMapping(value = "/corpus/prepare", method = RequestMethod.POST, headers = "Accept=application/json")
    public Corpus prepare(@RequestBody Query query) {

        if (query == null) throw new NullPointerException("Query should not be null");
        Corpus corpus = corpusBuilder.prepareCorpus(query);
        if (corpus != null) {
            ContactInfo contactInfo = new ContactInfo();
            contactInfo.setContactEmail(tokenEmail);
            corpus.getCorpusInfo().setContactInfo(contactInfo);
        }
        return corpus;
    }

    @RequestMapping(value = "/corpus/build", method = RequestMethod.GET, headers = "Accept=application/json")
    public void build(@RequestParam(value = "id") String id) {

        Corpus corpus = new Corpus();
        MetadataHeaderInfo metadataHeaderInfo = new MetadataHeaderInfo();
        MetadataIdentifier metadataIdentifier = new MetadataIdentifier();
        metadataIdentifier.setValue(id);
        metadataHeaderInfo.setMetadataRecordIdentifier(metadataIdentifier);
        corpus.setMetadataHeaderInfo(metadataHeaderInfo);
        corpusBuilder.buildCorpus(corpus);
        System.out.println(corpus.getMetadataHeaderInfo().getMetadataRecordIdentifier().getValue());
    }

    @RequestMapping(value = "/corpus/build", method = RequestMethod.POST, headers = "Accept=application/json")
    public void build(@RequestBody Corpus corpus) {

        corpusBuilder.buildCorpus(corpus);
    }

    @RequestMapping(value = "/corpus/status", method = RequestMethod.GET, headers = "Accept=application/json")
    public CorpusStatus status(@RequestParam(value = "id") String id) {
        return corpusBuilder.getStatus(id);
    }

    @RequestMapping(value = "/corpus/cancel", method = RequestMethod.GET, headers = "Accept=application/json")
    public void cancel(@RequestParam(value = "id") String id) {
        corpusBuilder.cancelProcess(id);
    }

    @RequestMapping(value = "/corpus/delete", method = RequestMethod.GET, headers = "Accept=application/json")
    public void delete(@RequestParam(value = "id") String id) {
        corpusBuilder.deleteCorpus(id);
    }
}