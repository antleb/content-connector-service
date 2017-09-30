package eu.openminted.content.service.messages;

import eu.openminted.content.service.mail.EmailMessage;
import eu.openminted.content.service.mail.JavaMailer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

@Component("jmsConsumer")
public class JMSConsumer {
    private static Logger log = Logger.getLogger(JMSConsumer.class.getName());

    @Autowired
    private JavaMailer javaMailer;

    @JmsListener(containerFactory = "jmsListenerContainerFactory", destination = "${jms.content.email.topic}")
    public void onMessage(EmailMessage emailMessage) {

        if (emailMessage == null
                || emailMessage.getRecipient() == null
                || emailMessage.getRecipient().isEmpty()) return;

        log.info("Sending email at " + emailMessage.getRecipient());
        javaMailer.sendEmail(emailMessage.getRecipient(), emailMessage.getSubject(), emailMessage.getText());
    }
}

