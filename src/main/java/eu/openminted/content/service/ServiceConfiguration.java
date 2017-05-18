package eu.openminted.content.service;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan({"eu.openminted.content", "eu.openminted.content.connector.core"})
public class ServiceConfiguration {
}
