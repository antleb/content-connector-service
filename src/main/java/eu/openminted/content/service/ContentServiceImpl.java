package eu.openminted.content.service;

import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.registry.domain.Facet;
import eu.openminted.registry.domain.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by antleb on 11/16/16.
 */
@Component
public class ContentServiceImpl implements ContentService {

    @Autowired(required = false)
    private List<ContentConnector> contentConnectors;

    @Override
    public SearchResult search(Query query) {
        SearchResult result = new SearchResult();
        Facet sourceFacet = new Facet();

        result.setFacets(new ArrayList<>());

        sourceFacet.setField("source");
        sourceFacet.setLabel("Content Source");
        sourceFacet.setValues(new ArrayList<>());

        if (contentConnectors != null) {
            for (ContentConnector connector:contentConnectors) {
                SearchResult res = connector.search(query);
                Value value = new Value();

                value.setValue(connector.getSourceName());
                value.setCount(res.getTotalHits());

                sourceFacet.getValues().add(value);

                result = merge(result, res);
            }
        }

        result.getFacets().add(sourceFacet);

        return result;
    }

    private SearchResult merge(SearchResult result1, SearchResult result2) {
        SearchResult searchResult = new SearchResult();

        searchResult.setTotalHits(result1.getTotalHits() + result2.getTotalHits());
        searchResult.setFrom(result1.getFrom());
        searchResult.setTo(result1.getTo());

        searchResult.setFacets(mergeFacets(result1.getFacets(), result2.getFacets()));

        //TODO: how to merge results? do we need to?

        return searchResult;
    }

    private List<Facet> mergeFacets(List<Facet> f1, List<Facet> f2) {
//        Map<String, Facet> mf1 = new HashMap<>();

        return mergeFacets(
                f1.stream().collect(Collectors.toMap(f -> f.getField(), f -> f)),
                f2.stream().collect(Collectors.toMap(f -> f.getField(), f -> f))).
                    values().stream().collect(Collectors.toList());
    }

    private Map<String, Facet> mergeFacets(Map<String, Facet> f1, Map<String, Facet> f2) {
        Map<String, Facet> temp = new HashMap<>();

        for (Map.Entry<String, Facet> e:f1.entrySet()) {
            temp.put(e.getKey(), e.getValue());
        }

        for (Map.Entry<String, Facet> e:f2.entrySet()) {
            if (temp.containsKey(e.getKey()))
                temp.put(e.getKey(), mergeFacet(temp.get(e.getKey()), e.getValue()));
            else
                temp.put(e.getKey(), e.getValue());
        }

        return temp;
    }

    private Facet mergeFacet(Facet f1, Facet f2) {
        Facet f = new Facet();
        Map<String, Integer> temp = new HashMap();

        f.setField(f1.getField());
        f.setLabel(f1.getLabel());
        f.setValues(new ArrayList<>());

        for (Value v:f1.getValues())
            temp.put(v.getValue(), v.getCount());

        for (Value v:f2.getValues())
            if (temp.containsKey(v.getValue()))
                temp.put(v.getValue(), v.getCount() + temp.get(v.getValue()));
            else
                temp.put(v.getValue(), v.getCount());

        for (Map.Entry<String, Integer> e:temp.entrySet()) {
            Value v = new Value();

            v.setValue(e.getKey());
            v.setCount(e.getValue());

            f.getValues().add(v);
        }

        Collections.sort(f.getValues());

        return f;
    }

    public List<ContentConnector> getContentConnectors() {
        return contentConnectors;
    }

    public void setContentConnectors(List<ContentConnector> contentConnectors) {
        this.contentConnectors = contentConnectors;
    }
}
