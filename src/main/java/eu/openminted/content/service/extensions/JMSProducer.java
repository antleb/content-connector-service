package eu.openminted.content.service.extensions;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.joda.time.DateTime;

import javax.jms.*;
import java.util.Random;

public class JMSProducer implements Runnable {
    private String brokerUrl;
    private String queueName;
    private String message;

    public JMSProducer(String brokerUrl, String queueName) {
        this.brokerUrl = brokerUrl;
        this.queueName = queueName;
    }

    @Override
    public void run() {
        try {
            // Create a ConnectionFactory
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                    brokerUrl);

            // Create a Connection
            Connection connection = connectionFactory.createConnection();
            connection.start();

            // Create a Session
            Session session = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            Destination destination = session.createQueue(queueName);

            // Create a MessageProducer from the Session to the Topic or
            // Queue
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            // Create a messages
            TextMessage textMessage = session.createTextMessage(message);

            // Tell the producer to send the message
            System.out.println("Sent message: " + textMessage.hashCode()
                    + " : " + Thread.currentThread().getName());
            producer.send(textMessage);

            // Clean up
            session.close();
            connection.close();
        } catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
