package eu.openminted.content.service.messages;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openminted.content.service.mail.EmailMessage;
import eu.openminted.content.service.mail.JavaMailer;
import eu.openminted.corpus.CorpusBuildingState;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.jms.*;
import java.io.IOException;

@Component
public class JMSConsumer implements ExceptionListener, MessageListener {
    private static Logger log = Logger.getLogger(JMSConsumer.class.getName());

    @org.springframework.beans.factory.annotation.Value("${jms.content.corpus.topic}")
    private String topicName;

    @Autowired
    private ActiveMQConnectionFactory connectionFactory;

    @Autowired
    private JavaMailer javaMailer;

    private Connection connection;

    private Session session;

    public void listen() {
        try {
            // Create a Connection
            connection = connectionFactory.createConnection();
            connection.setExceptionListener(this);

            // Set unique clientID to connection prior to connect
            connection.setClientID(Integer.toString(this.hashCode()));
            connection.start();


            // Create a Session
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

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

        try {
            session.close();
            connection.close();
        } catch (JMSException e) {
            log.error("JMS Exception occurred while shutting down client.", e);
        }
    }

    @Override
    public void onMessage(Message message) {
        if (message instanceof TextMessage) {
            try {
                TextMessage textMessage = (TextMessage) message;
                String text = textMessage.getText();
                log.info("Message Received: " + text);

                try {
                    JMSMessage messageReceived = new ObjectMapper().readValue(text, JMSMessage.class);

                    if (messageReceived.getType().equals(CorpusBuildingState.class.toString())) {
                        CorpusBuildingState corpusBuildingState = new ObjectMapper().readValue(messageReceived.getMessage(), CorpusBuildingState.class);
                        log.info("State of corpus building: " + corpusBuildingState);
                    } else if (messageReceived.getType().equals(EmailMessage.class.toString())) {
                        EmailMessage emailMessage = new ObjectMapper().readValue(messageReceived.getMessage(), EmailMessage.class);
                        if (emailMessage == null
                                || emailMessage.getRecipient() == null
                                || emailMessage.getRecipient().isEmpty()) return;
                        javaMailer.sendEmail(emailMessage.getRecipient(), emailMessage.getSubject(), emailMessage.getText());
                    }
                } catch (IOException e) {
                    log.error(e);
                }


//                if (text.contains("email")) {
//                    // Recipient's email ID needs to be mentioned.
//                    String to = "";
//                    String subject = "";
//
//                    Matcher match = Pattern.compile("^email<(.*)>subject<(.*)>(.*$)").matcher(text);
//                    if (match.find()) {
//                        to = match.group(1);
//                        subject = match.group(2);
//                        text = match.group(3);
//                    }
//
//                    if (to.isEmpty()) return;
//
//                    javaMailer.sendEmail(to, subject, text);
//                }
            } catch (JMSException e) {
                log.error("Error Receiving Message", e);
            }
        } else {
            log.info("Message Received: " + message);
        }
    }
}

