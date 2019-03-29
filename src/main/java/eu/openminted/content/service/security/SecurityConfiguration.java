package eu.openminted.content.service.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.session.web.http.CookieSerializer;
import org.springframework.session.web.http.DefaultCookieSerializer;

@Configuration
public class SecurityConfiguration {
    private static Logger log = LoggerFactory.getLogger(SecurityConfiguration.class);

    @Bean
    public CookieSerializer cookieSerializer() {

        try {
            DefaultCookieSerializer serializer = new DefaultCookieSerializer();
            serializer.setCookieName("SESSION"); // <1>
            serializer.setCookiePath("/"); // <2>
            return serializer;
        } catch (Exception e) {
            log.error("Error in SecurityConfiguration: ", e);
        }
        return null;
    }
}
