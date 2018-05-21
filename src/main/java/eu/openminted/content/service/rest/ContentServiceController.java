package eu.openminted.content.service.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.content.service.ContentService;
import eu.openminted.content.service.ServiceStatus;
import eu.openminted.content.service.database.CorpusBuilderInfoDao;
import eu.openminted.content.service.exception.ResourceNotFoundException;
import eu.openminted.content.service.exception.ServiceAuthenticationException;
import eu.openminted.content.service.model.CorpusBuilderInfoModel;
import eu.openminted.corpus.CorpusBuilder;
import eu.openminted.corpus.CorpusStatus;
import eu.openminted.registry.domain.Corpus;
import org.apache.log4j.Logger;
import org.mitre.openid.connect.model.OIDCAuthenticationToken;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@RestController
public class ContentServiceController {
    private static Logger log = Logger.getLogger(ContentServiceController.class.getName());

    @Autowired
    private CorpusBuilderInfoDao corpusBuilderInfoDao;

    @Autowired
    private ContentService contentService;

    @Autowired
    private CorpusBuilder corpusBuilder;

    /**
     * Method for searching for publications with facets as string
     * @param facets the facets separated with commas
     * @return the SearchResult of the current query
     */
    @RequestMapping(value = "/content/browse", method = RequestMethod.GET, headers = "Accept=application/json")
    public SearchResult browse(@RequestParam(value = "facets") String facets) throws IOException {

        List<String> facetList = Arrays.asList(facets.split(","));
        return contentService.search(new Query("", new HashMap<>(), facetList, 0, 10));
    }

    /**
     * Method for searching publications with a specific query
     * @param query the query to request publications
     * @return the SearchResult of the current query
     */
    @RequestMapping(value = "/content/browse", method = RequestMethod.POST, headers = "Accept=application/json")
    public SearchResult browse(@RequestBody Query query) throws IOException {

        if (query == null) throw new ResourceNotFoundException();
        return contentService.search(query);
    }

    /**
     * Returns the status of the specific service
     * @return the status of the service
     */
    @RequestMapping(value = "/content/status", method = RequestMethod.GET, headers = "Accept=application/json")
    public ServiceStatus status() {

        return contentService.status();
    }

    /**
     * Method that prepares a corpus for building with specific facets
     * @param facets a string containing the facets separated with commas
     * @return the corpus that is to be built
     */
    @PreAuthorize("hasRole('ROLE_USER')")
    @RequestMapping(value = "/corpus/prepare", method = RequestMethod.GET, headers = "Accept=application/json")
    public Corpus prepare(@RequestParam(value = "facets") String facets) {
        if (facets == null || facets.isEmpty()) throw new ResourceNotFoundException();

        if (SecurityContextHolder.getContext() == null || !(SecurityContextHolder.getContext().getAuthentication() instanceof OIDCAuthenticationToken))
            throw new ServiceAuthenticationException();

        List<String> facetList = new ArrayList<>(Arrays.asList(facets.split(",")));
        Query query = new Query("*:*", new HashMap<>(), facetList, 0, 1);
        return corpusBuilder.prepareCorpus(query);
    }

    /**
     * Method that prepares a corpus for building
     * @param query the query that will browse for publications to add to the corpus
     * @return the corpus that is to be built
     */
    @PreAuthorize("hasRole('ROLE_USER')")
    @RequestMapping(value = "/corpus/prepare", method = RequestMethod.POST, headers = "Accept=application/json")
    public Corpus prepare(@RequestBody Query query) {

        if (query == null) throw new ResourceNotFoundException();

        if (SecurityContextHolder.getContext() == null || !(SecurityContextHolder.getContext().getAuthentication() instanceof OIDCAuthenticationToken))
            throw new ServiceAuthenticationException();

        if(query.getKeyword() == null || query.getKeyword().isEmpty()){
            query.setKeyword("*:*");
        }
        return corpusBuilder.prepareCorpus(query);
    }

    /**
     * Builds an existing, already prepared, corpus based on its id
     * @param id the id of an existing corpus
     * @throws IOException In case corpus cannot be retrieved by its json form
     */
    @PreAuthorize("hasRole('ROLE_USER')")
    @RequestMapping(value = "/corpus/build", method = RequestMethod.GET, headers = "Accept=application/json")
    public void build(@RequestParam(value = "id") String id) throws IOException {

        if (id == null || id.isEmpty()) throw new ResourceNotFoundException();

        if (SecurityContextHolder.getContext() == null || !(SecurityContextHolder.getContext().getAuthentication() instanceof OIDCAuthenticationToken))
            throw new ServiceAuthenticationException();

        CorpusBuilderInfoModel model = corpusBuilderInfoDao.find(id);
        ObjectMapper mapper = new ObjectMapper();
        Corpus corpus = mapper.readValue(model.getCorpus(), Corpus.class);
        corpusBuilder.buildCorpus(corpus);
    }

    /**
     * Builds a corpus
     * @param corpus the corpus that is to be built
     */
    @PreAuthorize("hasRole('ROLE_USER')")
    @RequestMapping(value = "/corpus/build", method = RequestMethod.POST, headers = "Accept=application/json")
    public void build(@RequestBody Corpus corpus) {

        if (corpus == null
                || corpus.getMetadataHeaderInfo().getMetadataRecordIdentifier().getValue().isEmpty()) throw new ResourceNotFoundException();

        if (SecurityContextHolder.getContext() == null || !(SecurityContextHolder.getContext().getAuthentication() instanceof OIDCAuthenticationToken))
            throw new ServiceAuthenticationException();

        corpusBuilder.buildCorpus(corpus);
    }

    /**
     * Status of the current corpus building process
     * @param id the id of a specific corpus
     * @return the status of the building process
     */
    @RequestMapping(value = "/corpus/status", method = RequestMethod.GET, headers = "Accept=application/json")
    public CorpusStatus status(@RequestParam(value = "id") String id) {
        return corpusBuilder.getStatus(id);
    }

    /**
     * Cancels the current corpus building process
     * @param id the id of a specific corpus
     */
    @RequestMapping(value = "/corpus/cancel", method = RequestMethod.GET, headers = "Accept=application/json")
    public void cancel(@RequestParam(value = "id") String id) {
        corpusBuilder.cancelProcess(id);
    }

    /**
     * Deletes a specific corpus
     * @param id the id of a specific corpus
     */
    @RequestMapping(value = "/corpus/delete", method = RequestMethod.GET, headers = "Accept=application/json")
    public void delete(@RequestParam(value = "id") String id) {
        corpusBuilder.deleteCorpus(id);
    }

    /**
     * Information about the current user
     * @return retrieves information for the current logged in user
     */
    @RequestMapping(value = "/user/info", method = RequestMethod.GET)
    @PreAuthorize("hasRole('ROLE_USER')")
    public ResponseEntity<Object> user() {
        OIDCAuthenticationToken authentication = (OIDCAuthenticationToken) SecurityContextHolder.getContext().getAuthentication();
        return new ResponseEntity<>(authentication.getUserInfo().toJson().toString(), HttpStatus.OK);
    }
}