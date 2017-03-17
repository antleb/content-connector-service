package eu.openminted.content.service.extensions;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.jms.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class JMSConsumer implements ExceptionListener, MessageListener {
    private static Logger log = Logger.getLogger(JMSConsumer.class.getName());

    @org.springframework.beans.factory.annotation.Value("${jms.content.corpus.topic}")
    private String topicName;

    @org.springframework.beans.factory.annotation.Value("${mail.port}")
    private String mailPort;

    @org.springframework.beans.factory.annotation.Value("${mail.sender.id}")
    private String mailSenderId;

    @org.springframework.beans.factory.annotation.Value("${mail.username}")
    private String mailUsername;

    @org.springframework.beans.factory.annotation.Value("${mail.password}")
    private String mailPassword;

    @org.springframework.beans.factory.annotation.Value("${mail.smtp.host}")
    private String mailSmtpHost;

    @Autowired
    private ActiveMQConnectionFactory connectionFactory;

    @Autowired
    private JavaMailer javaMailer;

    public void listen() {
        try {
            // Create a Connection
            Connection connection = connectionFactory.createConnection();
            // Set unique clientID to connection prior to connect
            connection.setClientID(Integer.toString(this.hashCode()));
            connection.start();

            connection.setExceptionListener(this);

            // Create a Session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the Topic
            Topic topic = session.createTopic(topicName);

            // Create a TopicSubscriber from the Session to the Topic
            TopicSubscriber subscriber = session.createDurableSubscriber(topic, "SUBSCRIBER");
            subscriber.setMessageListener(this);

        } catch (Exception e) {
            log.error("Caught Exception: " + e);
        }
    }

    public synchronized void onException(JMSException ex) {
        log.error("JMS Exception occurred.  Shutting down client.");
    }

    @Override
    public void onMessage(Message message) {
        if (message instanceof TextMessage) {
            try {
                TextMessage textMessage = (TextMessage) message;
                String text = textMessage.getText();
                log.info("Message Received: " + text);

                if (text.contains("email")) {
                    // Recipient's email ID needs to be mentioned.
                    String to = "";
                    String subject = "";

                    Matcher match = Pattern.compile("^email<(.*)>subject<(.*)>(.*$)").matcher(text);
                    if (match.find()) {
                        to = match.group(1);
                        subject = match.group(2);
                        text = match.group(3);
                    }

                    if (to.isEmpty()) return;

                    javaMailer.sendEmail(to, subject, text);
                }
            } catch (JMSException e) {
                log.error("Error Receiving Message", e);
            }
        } else {
            log.info("Message Received: " + message);
        }
    }
}

