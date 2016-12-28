package eu.openminted.content.service.extensions;

import eu.openminted.content.connector.SearchResult;
import eu.openminted.registry.domain.Facet;
import eu.openminted.registry.domain.Value;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by constantine on 23/12/2016.
 */
public class SearchResultExtension extends SearchResult {

    public SearchResult merge(SearchResult result) {
        this.setTotalHits(this.getTotalHits() + result.getTotalHits());

        this.setFacets(mergeFacets(this.getFacets(), result.getFacets()));

        //TODO: how to merge results? do we need to?

        return this;
    }

    private List<Facet> mergeFacets(List<Facet> f1, List<Facet> f2) {
//        Map<String, Facet> mf1 = new HashMap<>();

        //noinspection Convert2MethodRef
        return mergeFacets(
                f1.stream().collect(Collectors.toMap(f -> f.getField(), f -> f)),
                f2.stream().collect(Collectors.toMap(f -> f.getField(), f -> f))).
                values().stream().collect(Collectors.toList());
    }

    private Map<String, Facet> mergeFacets(Map<String, Facet> f1, Map<String, Facet> f2) {
        Map<String, Facet> temp = new HashMap<>();

        for (Map.Entry<String, Facet> e : f1.entrySet()) {
            temp.put(e.getKey(), e.getValue());
        }

        for (Map.Entry<String, Facet> e : f2.entrySet()) {
            if (temp.containsKey(e.getKey()))
                temp.put(e.getKey(), mergeFacet(temp.get(e.getKey()), e.getValue()));
            else
                temp.put(e.getKey(), e.getValue());
        }

        return temp;
    }

    private Facet mergeFacet(Facet f1, Facet f2) {
        Facet f = new Facet();
        Map<String, Integer> temp = new HashMap<>();

        f.setField(f1.getField());
        f.setLabel(f1.getLabel());
        f.setValues(new ArrayList<>());

        for (Value v : f1.getValues())
            temp.put(v.getValue(), v.getCount());

        for (Value v : f2.getValues())
            if (temp.containsKey(v.getValue()))
                temp.put(v.getValue(), v.getCount() + temp.get(v.getValue()));
            else
                temp.put(v.getValue(), v.getCount());

        for (Map.Entry<String, Integer> e : temp.entrySet()) {
            Value v = new Value();

            v.setValue(e.getKey());
            v.setCount(e.getValue());

            f.getValues().add(v);
        }

        Collections.sort(f.getValues());

        return f;
    }
}
