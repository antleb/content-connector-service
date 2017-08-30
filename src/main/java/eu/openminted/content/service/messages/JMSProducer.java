package eu.openminted.content.service.messages;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.jms.*;

@Component
public class JMSProducer {
    private static Logger log = Logger.getLogger(JMSProducer.class.getName());

    @org.springframework.beans.factory.annotation.Value("${jms.content.corpus.topic}")
    private String topic;

    @Autowired
    private ActiveMQConnectionFactory connectionFactory;

    public void send(String message) {
        try {
            Connection connection = connectionFactory.createConnection();
            connection.start();
            // Create a Session
            Session session = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);

            // Create the destination (with Topic)
            Destination destination = session.createTopic(topic);

            // Create a MessageProducer from the Session to the Topic
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            // Create a messages
            TextMessage textMessage = session.createTextMessage(message);

            // Tell the producer to send the message
            log.info("Sending Message: " + textMessage);
            producer.send(textMessage);

            // Clean up
            session.close();
            connection.close();
        } catch (JMSException e) {
            log.error("Caught Exception", e);
        }
    }

    public void sendMessage(String type, Object object) {
        try {
            JMSMessage jmsMessage = new JMSMessage();
            jmsMessage.setType(type);
            jmsMessage.setMessage(new ObjectMapper().writeValueAsString(object));
            String messageJson = new ObjectMapper().writeValueAsString(jmsMessage);
            new Thread(() -> this.send(messageJson)).start();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
