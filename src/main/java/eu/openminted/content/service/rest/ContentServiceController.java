package eu.openminted.content.service.rest;

import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.content.service.ContentService;
import eu.openminted.content.service.dao.CorpusDao;
import eu.openminted.corpus.CorpusBuilder;
import eu.openminted.registry.domain.Corpus;
import eu.openminted.registry.domain.MetadataHeaderInfo;
import eu.openminted.registry.domain.MetadataIdentifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by antleb on 11/16/16.
 */
@RestController
public class ContentServiceController {
    @Autowired
    private CorpusDao corpusDao;

    @Autowired
    private ContentService contentService;

    @Autowired
    private CorpusBuilder corpusBuilder;

    @RequestMapping(value = "/content/browse", method = RequestMethod.GET, headers = "Accept=application/json")
    public SearchResult browse(@RequestParam(value = "facets") String facets) {

        List<String> facetList = Arrays.asList(facets.split(","));
        return contentService.search(new Query("*:*", new HashMap<>(), facetList, 0, 10));
    }

    @RequestMapping(value = "/content/browse", method = RequestMethod.POST, headers = "Accept=application/json")
    public SearchResult browse(@RequestBody Query query) {

        return contentService.search(query);
    }

    @RequestMapping(value = "/corpus/prepare", method = RequestMethod.GET, headers = "Accept=application/json")
    public Corpus prepare(@RequestParam(value = "facets") String facets) {

        List<String> facetList = new ArrayList<>(Arrays.asList(facets.split(",")));
        return corpusBuilder.prepareCorpus(new Query("*:*", new HashMap<>(), facetList, 0, 10));
    }

    @RequestMapping(value = "/corpus/prepare", method = RequestMethod.POST, headers = "Accept=application/json")
    public Corpus prepare(@RequestBody Query query) {

        return corpusBuilder.prepareCorpus(query);
    }

    @RequestMapping(value = "/corpus/build", method = RequestMethod.GET, headers = "Accept=application/json")
    public String build(@RequestParam(value = "id") String id) {
        try {
            Corpus corpus = new Corpus();
            MetadataHeaderInfo metadataHeaderInfo = new MetadataHeaderInfo();
            MetadataIdentifier metadataIdentifier = new MetadataIdentifier();
            metadataIdentifier.setValue(id);
            metadataHeaderInfo.setMetadataRecordIdentifier(metadataIdentifier);
            corpus.setMetadataHeaderInfo(metadataHeaderInfo);
            corpusBuilder.buildCorpus(corpus);
            System.out.println(corpus.getMetadataHeaderInfo().getMetadataRecordIdentifier().getValue());
        } catch (Exception ex) {
            id = ex.getMessage();
        }
        return id;
    }
}