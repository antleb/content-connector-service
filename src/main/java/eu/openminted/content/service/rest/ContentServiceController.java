package eu.openminted.content.service.rest;

import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.content.service.ContentService;
import eu.openminted.content.service.ServiceStatus;
import eu.openminted.content.service.database.CorpusBuilderInfoDao;
import eu.openminted.content.service.exception.ResourceNotFoundException;
import eu.openminted.content.service.exception.ServiceAuthenticationException;
import eu.openminted.corpus.CorpusBuilder;
import eu.openminted.corpus.CorpusStatus;
import eu.openminted.registry.domain.*;
import org.apache.log4j.Logger;
import org.mitre.openid.connect.model.OIDCAuthenticationToken;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

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

    @Value("${authentication.token.email:test.espas@gmail.com}")
    private String tokenEmail;

    @Value("${authentication.token.name:omtd-user}")
    private String tokenName;

    @RequestMapping(value = "/content/browse", method = RequestMethod.GET, headers = "Accept=application/json")
    public SearchResult browse(@RequestParam(value = "facets") String facets) {

        List<String> facetList = Arrays.asList(facets.split(","));
        return contentService.search(new Query("", new HashMap<>(), facetList, 0, 10));
    }

    @RequestMapping(value = "/content/browse", method = RequestMethod.POST, headers = "Accept=application/json")
    public SearchResult browse(@RequestBody Query query) {

        if (query == null) throw new ResourceNotFoundException();
        return contentService.search(query);
    }

    @RequestMapping(value = "/content/status", method = RequestMethod.GET, headers = "Accept=application/json")
    public ServiceStatus status() {

        return contentService.status();
    }

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

    @PreAuthorize("hasRole('ROLE_USER')")
    @RequestMapping(value = "/corpus/prepare", method = RequestMethod.POST, headers = "Accept=application/json")
    public Corpus prepare(@RequestBody Query query) {

        if (query == null) throw new ResourceNotFoundException();

        if (SecurityContextHolder.getContext() == null || !(SecurityContextHolder.getContext().getAuthentication() instanceof OIDCAuthenticationToken))
            throw new ServiceAuthenticationException();

        Corpus corpus = corpusBuilder.prepareCorpus(query);
        if (corpus != null) {
            populateCorpus(corpus);
        }
        return corpus;
    }

    @PreAuthorize("hasRole('ROLE_USER')")
    @RequestMapping(value = "/corpus/build", method = RequestMethod.GET, headers = "Accept=application/json")
    public void build(@RequestParam(value = "id") String id) {

        if (id == null || id.isEmpty()) throw new ResourceNotFoundException();

        if (SecurityContextHolder.getContext() == null || !(SecurityContextHolder.getContext().getAuthentication() instanceof OIDCAuthenticationToken))
            throw new ServiceAuthenticationException();

        Corpus corpus = new Corpus();
        MetadataHeaderInfo metadataHeaderInfo = new MetadataHeaderInfo();
        MetadataIdentifier metadataIdentifier = new MetadataIdentifier();
        metadataIdentifier.setValue(id);
        metadataHeaderInfo.setMetadataRecordIdentifier(metadataIdentifier);
        corpus.setMetadataHeaderInfo(metadataHeaderInfo);
        corpusBuilder.buildCorpus(corpus);
    }

    @PreAuthorize("hasRole('ROLE_USER')")
    @RequestMapping(value = "/corpus/build", method = RequestMethod.POST, headers = "Accept=application/json")
    public void build(@RequestBody Corpus corpus) {

        if (corpus == null) throw new ResourceNotFoundException();

        if (SecurityContextHolder.getContext() == null || !(SecurityContextHolder.getContext().getAuthentication() instanceof OIDCAuthenticationToken))
            throw new ServiceAuthenticationException();

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

    @RequestMapping(value = "/user/info", method = RequestMethod.GET)
    @PreAuthorize("hasRole('ROLE_USER')")
    public ResponseEntity<Object> user() {
//        return new ResponseEntity<>(SecurityContextHolder.getContext().getAuthentication(), HttpStatus.OK);
        OIDCAuthenticationToken authentication = (OIDCAuthenticationToken) SecurityContextHolder.getContext().getAuthentication();
        return new ResponseEntity<>(authentication.getUserInfo().toJson().toString(), HttpStatus.OK);
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
}