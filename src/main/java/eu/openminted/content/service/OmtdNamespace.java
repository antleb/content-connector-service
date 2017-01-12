package eu.openminted.content.service;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import java.util.Iterator;

/**
 * Created by Constantine on 10/28/14.
 */
public class OmtdNamespace implements NamespaceContext {

    @Override
    public String getNamespaceURI(String prefix) {

        if (prefix == null) {
            throw new NullPointerException("Null prefix");

        } else if ("xlink".equals(prefix)) {
            return "http://www.w3.org/1999/xlink";

        } else if ("gml".equals(prefix)) {
            return "http://www.opengis.net/gml/3.2";

        } else if ("gmd".equals(prefix)) {
            return "http://www.isotc211.org/2005/gmd";

        } else if ("gmi".equals(prefix)) {
            return "http://www.isotc211.org/2005/gmi";

        } else if ("gco".equals(prefix)) {
            return "http://www.isotc211.org/2005/gco";

        } else if ("xsi".equals(prefix)) {
            return "http://www.w3.org/2001/XMLSchema-instance";

        } else if ("om".equals(prefix)) {
            return "http://www.opengis.net/om/2.0";

        } else if ("xml".equals(prefix)) {
            return XMLConstants.XML_NS_URI;

        } else if ("ns1".equals(prefix)) {
            return "http://www.meta-share.org/OMTD-SHARE_XMLSchema";

        }

        return XMLConstants.DEFAULT_NS_PREFIX;
    }

    // This method isn't necessary for XPath processing.
    public String getPrefix(String uri) {
        throw new UnsupportedOperationException();
    }

    // This method isn't necessary for XPath processing either.
    public Iterator<Object> getPrefixes(String uri) {
        throw new UnsupportedOperationException();
    }

}
