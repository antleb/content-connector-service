package eu.openminted.content.service.threads;

import eu.openminted.content.connector.SearchResult;
import eu.openminted.registry.core.domain.Value;

/* Wrapper-Class: return searchResult and sourceValue as a single entity. Used in Callable & ThreadpoolWrapper as generic */
public class QueryResult {

    private SearchResult searchResult;

    private Value sourceValue;

    public QueryResult(SearchResult searchResult, Value sourceValue) {
        this.searchResult = searchResult;
        this.sourceValue = sourceValue;
    }

    public SearchResult getSearchResult() {
        return searchResult;
    }

    public void setSearchResult(SearchResult searchResult) {
        this.searchResult = searchResult;
    }

    public Value getSourceValue() {
        return sourceValue;
    }

    public void setSourceValue(Value sourceValue) {
        this.sourceValue = sourceValue;
    }
}
