package eu.openminted.content.service.extensions;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

@Component
public class JavaMailer {
    private Properties properties;
    private long tokenExpires;


    @PostConstruct
    public void init() {
        properties = new Properties();
        properties.put("mail.smtp.ssl.enable", "true");
        properties.put("mail.smtp.sasl.enable", "true");
        properties.put("mail.smtp.sasl.mechanisms", "XOAUTH2");
        properties.put("mail.smtp.auth.login.disable", "true");
        properties.put("mail.smtp.auth.plain.disable", "true");
        properties.put("mail.debug", "true");

        tokenExpires = 1458168133864L;
    }

    public void sendEmail(String to, String subject, String text) {
        String TOKEN_URL = "https://www.googleapis.com/oauth2/v4/token";
        String oauthClientId = "873237151023-mld63l7iboss5pe7i2cv1p8cnm0jgbb6.apps.googleusercontent.com";
        String oauthSecret = "H4CT3RtQhvkZO4YK5z7ToDHb";
        String refreshToken = "1/WsDHGk1GY-BgTz6Th4FYVTjJ2ATc9oFVhJ6u-dcMkzs";
        String accessToken = "ya29.GlsQBCqYNogRT3jTcFzf4f1H7viuEUKyndZ7OAvke9fiZp0tTULyzprDWTj09pWU6SxpPaEGOhYnrN4zWuKmastD7lVR1WgH3XjDs85hfX4Z_ZVO3C3VGiJiArs-";
        String username = "test.espas@gmail.com";

        if (System.currentTimeMillis() > tokenExpires) {
            try {
                String request = "client_id=" + URLEncoder.encode(oauthClientId, "UTF-8")
                        + "&client_secret=" + URLEncoder.encode(oauthSecret, "UTF-8")
                        + "&refresh_token=" + URLEncoder.encode(refreshToken, "UTF-8")
                        + "&grant_type=refresh_token";
                HttpURLConnection conn = (HttpURLConnection) new URL(TOKEN_URL).openConnection();
                conn.setDoOutput(true);
                conn.setRequestMethod("POST");
                PrintWriter out = new PrintWriter(conn.getOutputStream());
                out.print(request); // note: println causes error
                out.flush();
                out.close();
                conn.connect();

                try {
                    HashMap<String, Object> result;
                    result = new ObjectMapper().readValue(conn.getInputStream(), new TypeReference<HashMap<String, Object>>() {
                    });
                    accessToken = (String) result.get("access_token");
                    tokenExpires = System.currentTimeMillis() + (((Number) result.get("expires_in")).intValue() * 1000);
                } catch (IOException e) {
                    String line;
                    BufferedReader in = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
                    while ((line = in.readLine()) != null) {
                        System.out.println(line);
                    }
                    System.out.flush();
                }

                Session session = Session.getInstance(properties);
                Message msg = new MimeMessage(session);
                msg.setFrom(new InternetAddress(username));
                msg.addRecipient(Message.RecipientType.TO, new InternetAddress(to));

                msg.setSubject(subject);
                msg.setSentDate(new Date());
                msg.setText(text);
                msg.saveChanges();
                Transport transport = session.getTransport("smtp");
                transport.connect("smtp.gmail.com", username, accessToken);
                transport.sendMessage(msg, msg.getAllRecipients());
                transport.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
