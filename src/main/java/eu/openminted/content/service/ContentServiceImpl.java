package eu.openminted.content.service;

import com.google.common.collect.Lists;
import eu.openminted.content.connector.ContentConnector;
import eu.openminted.content.connector.Query;
import eu.openminted.content.connector.SearchResult;
import eu.openminted.registry.domain.Facet;
import eu.openminted.registry.domain.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class ContentServiceImpl implements ContentService {

    @Autowired(required = false)
    private List<ContentConnector> contentConnectors;

    @Override
    public SearchResult search(Query query) {

        if (query == null) {
            query = new Query("*:*", new HashMap<>(), new ArrayList<>(), 0, 1);
        } else if (query.getKeyword() == null || query.getKeyword().isEmpty()) {
            query.setKeyword("*:*");
        }

        query.setFrom(0);
        query.setTo(1);

        if (query.getFacets() == null) query.setFacets(new ArrayList<>());

        if (!containsIgnoreCase(query.getFacets(), "publicationType")) query.getFacets().add("publicationType");
        if (!containsIgnoreCase(query.getFacets(), "publicationYear")) query.getFacets().add("publicationYear");
        if (!containsIgnoreCase(query.getFacets(), "rightsStmtName")) query.getFacets().add("rightsStmtName");
        if (!containsIgnoreCase(query.getFacets(), "Licence")) query.getFacets().add("Licence");
        if (!containsIgnoreCase(query.getFacets(), "documentLanguage")) query.getFacets().add("documentLanguage");

        SearchResult result = new SearchResult();
        Facet sourceFacet = new Facet();

        result.setFacets(new ArrayList<>());

        sourceFacet.setField("source");
        sourceFacet.setLabel("Content Source");
        sourceFacet.setValues(new ArrayList<>());

        if (contentConnectors != null) {
            for (ContentConnector connector : contentConnectors) {
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
        if (f1 != null && f2 != null)
            return mergeFacets(
                    f1.stream().collect(Collectors.toMap(Facet::getField, f -> f)),
                    f2.stream().collect(Collectors.toMap(Facet::getField, f -> f))).
                    values().stream().collect(Collectors.toList());

        if (f1 == null && f2 == null) return new ArrayList<>();

        if (f1 == null) return f2;
        else return f1;
    }

    private Map<String, Facet> mergeFacets(Map<String, Facet> f1, Map<String, Facet> f2) {
        Map<String, Facet> temp = new HashMap<>();

        for (Map.Entry<String, Facet> e : f1.entrySet()) {
            if (e.getKey().equalsIgnoreCase("publicationyear")) {
                temp.put(e.getKey(), getPublicationYearFacet(e.getValue()));
            } else
                temp.put(e.getKey(), e.getValue());
        }

        for (Map.Entry<String, Facet> e : f2.entrySet()) {
            Facet facet;

            if (e.getKey().equalsIgnoreCase("publicationyear")) {
                facet = getPublicationYearFacet(e.getValue());
            } else {
                facet = e.getValue();
            }

            if (temp.containsKey(e.getKey())) {
                temp.put(e.getKey(), mergeFacet(temp.get(e.getKey()), facet));
            } else {
                temp.put(e.getKey(), facet);
            }
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

    public List<ContentConnector> getContentConnectors() {
        return contentConnectors;
    }

    public void setContentConnectors(List<ContentConnector> contentConnectors) {
        this.contentConnectors = contentConnectors;
    }

    private boolean containsIgnoreCase(List<String> list, String keyword) {

        for (String item : list) {
            if (item.equalsIgnoreCase(keyword)) return true;
        }
        return false;
    }

    private Facet getPublicationYearFacet(Facet publicationYearFacet) {
        Facet facet = new Facet();
        facet.setField(publicationYearFacet.getField());
        facet.setLabel(publicationYearFacet.getLabel());
        facet.setValues(mergeValues(publicationYearFacet));
        return facet;
    }

    private List<Value> mergeValues(Facet facet) {
        Map<String, Value> tmpValues = new HashMap<>();

        for (Value value : facet.getValues()) {
            Value tmpValue = new Value();
            tmpValue.setCount(value.getCount());
            tmpValue.setValue(value.getValue().substring(0, 4));

            if (tmpValues.containsKey(tmpValue.getValue())) {
                int currentCount = tmpValues.get(tmpValue.getValue()).getCount();
                tmpValues.get(tmpValue.getValue()).setCount(currentCount + tmpValue.getCount());
            } else {
                tmpValues.put(tmpValue.getValue(), tmpValue);
            }
        }

        List<Value> values = Lists.newArrayList(tmpValues.values());
        Collections.sort(values);
        Collections.reverse(values);
        return values;
    }

    public void printFacet(Facet facet) {
        System.out.println("field:" + facet.getField());
        System.out.println("values:[");
        for (Value value : facet.getValues()) {
            System.out.println(value.getValue());
            System.out.println(value.getCount());
        }
        System.out.println("]");
    }
}
