package eu.openminted.content.service.faceting;

import java.util.HashMap;
import java.util.Map;

public class OmtdFacetInitializer {
    private Map<String, String> OmtdFacetLabels = new HashMap<>();
    private String PUBLICATION_TYPE = "publicationType";
    private String PUBLICATION_YEAR = "publicationYear";
    String PUBLISHER = "publisher";
    private String RIGHTS_STMT_NAME = "rightsStmtName";
    private String LICENCE = "licence";
    private String DOCUMENT_LANG = "documentLanguage";
    private String DOCUMENT_TYPE = "documentType";
    private String SOURCE = "source";

    public OmtdFacetInitializer() {
        OmtdFacetLabels.put(PUBLICATION_TYPE,"Publication Type");
        OmtdFacetLabels.put(PUBLICATION_YEAR,"Publication Year");
        OmtdFacetLabels.put(RIGHTS_STMT_NAME,"Rights Statement");
        OmtdFacetLabels.put(LICENCE,"Licence");
        OmtdFacetLabels.put(DOCUMENT_LANG,"Language");
        OmtdFacetLabels.put(DOCUMENT_TYPE,"Document Type");
        OmtdFacetLabels.put(SOURCE,"Content Source");
    }

    public Map<String, String> getOmtdFacetLabels() {
        return OmtdFacetLabels;
    }


}
