package eu.openminted.content.service.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.session.web.http.CookieSerializer;
import org.springframework.session.web.http.DefaultCookieSerializer;

@Configuration
public class SecurityConfiguration {
    @Bean
    public CookieSerializer cookieSerializer() {

        try {
            DefaultCookieSerializer serializer = new DefaultCookieSerializer();
            serializer.setCookieName("SESSION"); // <1>
            serializer.setCookiePath("/"); // <2>
            // serializer.setDomainNamePattern("^.+?\\.(\\w+\\.[a-z]+)$"); // <3>
            return serializer;
        } catch (Exception e) {
            System.out.println("\n\n\n\n aleko " + e);
        }
        return null;
    }
}
