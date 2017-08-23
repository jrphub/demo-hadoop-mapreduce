package com.sample.java.notification;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

import org.apache.commons.io.Charsets;
import org.apache.velocity.app.VelocityEngine;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.mail.javamail.MimeMessagePreparator;
import org.springframework.ui.velocity.VelocityEngineUtils;

public class SendEmail {

    private JavaMailSenderImpl mailSender;
    private Properties javaMailProperties;
    private VelocityEngine velocityEngine;
    private MimeMessagePreparator preparator;

    public void sendNotification() {
        mailSender = new JavaMailSenderImpl();
        preparator = new MimeMessagePreparator() {
            public void prepare(MimeMessage mimeMessage) throws Exception {
                prepareMail(mimeMessage);
            }

        };
        System.out.println("Email Service : Sending Email ...");
        mailSender.send(preparator);

    }

    private void prepareMail(MimeMessage mimeMessage)
            throws MessagingException {

        javaMailProperties = new Properties();
        MimeMessageHelper message = new MimeMessageHelper(mimeMessage);
        velocityEngine = new VelocityEngine();

        // velocityEngine properties
        velocityEngine.setProperty("resource.loader", "classpath");
        velocityEngine.setProperty("classpath.resource.loader.class",
                "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");

        // javaMailProperties
        javaMailProperties.setProperty("mail.smtp.ssl.enable", "true");
        javaMailProperties.setProperty("mail.smtp.socketFactory.port", "465");
        javaMailProperties.setProperty("mail.smtp.socketFactory.class",
                "javax.net.ssl.SSLSocketFactory");

        javaMailProperties.setProperty("mail.smtp.auth", "true");
        javaMailProperties.setProperty("mail.debug", "true");

        // JavaMailSenderImpl Properties
        mailSender.setHost("smtp.gmail.com");
        mailSender.setUsername("jyoti.pattnaik@saama.com");
        mailSender.setPassword("fqjkzafemzvqhhyu");

        // SSL Support
        mailSender.setPort(Integer.valueOf(465));
        mailSender.setProtocol("smtps");
        mailSender.setJavaMailProperties(javaMailProperties);

        // SimpleMailMessage
        message.setTo(new String[] { "jyoti.pattnaik@saama.com" });
        message.setCc(new String[] { "jyoti.pattnaik@saama.com" });
        message.setBcc(new String[] { "jyoti.pattnaik@saama.com" });
        message.setSubject("Test Mail");

        Map<String, String> emailContent = new HashMap<String, String>();
        emailContent.put("receiver", "jyoti.pattnaik@saama.com");

        Map<String, Object> emailMap = new HashMap<String, Object>();
        emailMap.put("email", emailContent);

        String text = VelocityEngineUtils.mergeTemplateIntoString(
                velocityEngine, "templates/email-default.html",
                Charsets.UTF_8.displayName(), emailMap);

        message.setText(text, true);
    }

    public static void main(String[] args) {
        SendEmail sendEmail = new SendEmail();
        sendEmail.sendNotification();
    }

}
