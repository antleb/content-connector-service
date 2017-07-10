package eu.openminted.content.service.faceting;

public enum FacetEnum {
    PUBLICATION_TYPE("publicationType"),
    PUBLICATION_YEAR("publicationYear"),
    PUBLISHER("publisher"),
    RIGHTS_STMT_NAME("rightsStmtName"),
    LICENCE("licence"),
    DOCUMENT_LANG("documentLanguage"),
    DOCUMENT_TYPE("documentType"),
    SOURCE("source");

    private final String value;

    FacetEnum(String v) {
        value = v;
    }

    public String value() {
        return value;
    }

    public static FacetEnum fromValue(String v) {
        for (FacetEnum c: FacetEnum.values()) {
            if (c.value.equalsIgnoreCase(v)) {
                return c;
            }
        }
        throw new IllegalArgumentException(v);
    }
}
