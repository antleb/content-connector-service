package eu.openminted.content.service.messages;

import eu.openminted.content.service.mail.EmailMessage;
import eu.openminted.registry.domain.connector.CorpusBuildingState;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

@Component
public class JMSProducer {
    private static Logger log = Logger.getLogger(JMSProducer.class.getName());

    @org.springframework.beans.factory.annotation.Value("${jms.corpus.state.topic:corpus.state}")
    private String stateTopic;

    @org.springframework.beans.factory.annotation.Value("${jms.content.email.topic}")
    private String emailTopic;

    @Autowired
    private JmsTemplate jmsTemplate;

    public void sendMessage(EmailMessage emailMessage) {
        jmsTemplate.convertAndSend(emailTopic, emailMessage);
    }

    public void sendMessage(CorpusBuildingState corpusBuildingState) {
        jmsTemplate.convertAndSend(stateTopic, corpusBuildingState);
    }
}
