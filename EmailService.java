package com.saama.mdp.core.notification.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.velocity.app.VelocityEngine;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.ui.velocity.VelocityEngineUtils;

import com.google.common.base.Charsets;
import com.saama.mdp.core.notification.persistence.entity.NotificationConfigDto;


public class EmailService {

    private JavaMailSenderImpl mailSender;
    private Properties javaMailProperties;
    private SimpleMailMessage message;
    private VelocityEngine velocityEngine;

    public void sendNotification(NotificationConfigDto notifConfig) {

        prepare(notifConfig);

        mailSender.setJavaMailProperties(javaMailProperties);
        
        System.out.println("Email Service : Sending Email ..." + notifConfig.getSubject());
        mailSender.send(message);

    }

    private void prepare(NotificationConfigDto notifConfig) {
        NotifUtils.initAppProps();
        Properties props = NotifUtils.getAppProps();

        mailSender = new JavaMailSenderImpl();
        javaMailProperties = new Properties();
        message = new SimpleMailMessage();
        velocityEngine = new VelocityEngine();
        velocityEngine.setProperty("resource.loader", "file");
        velocityEngine.setProperty("file.resource.loader.class", "org.apache.velocity.runtime.resource.loader.FileResourceLoader");
        velocityEngine.setProperty("file.resource.loader.path", "/templates");
        

        mailSender.setHost(props.getProperty("mail.host"));

        mailSender.setUsername(props.getProperty("mail.emailid"));
        mailSender.setPassword(props.getProperty("mail.password"));

        if (props.getProperty("mail.smtp.starttls.enable", "false")
                .equals("true")) {
            // TLS/STARTTLS Support
            mailSender.setPort(
                    Integer.valueOf(props.getProperty("mail.tls.port", "587")));
            mailSender.setProtocol(
                    props.getProperty("mail.transport.protocol", "smtp"));
            javaMailProperties.setProperty("mail.smtp.starttls.enable", "true");
        } else if (props.getProperty("mail.smtp.ssl.enable", "false")
                .equals("true")) {
            // SSL Support
            mailSender.setPort(
                    Integer.valueOf(props.getProperty("mail.ssl.port", "465")));
            mailSender.setProtocol(
                    props.getProperty("mail.transport.protocol", "smtps"));
            javaMailProperties.setProperty("mail.smtp.ssl.enable", "true");
            javaMailProperties.setProperty("mail.smtp.socketFactory.port",
                    props.getProperty("mail.smtp.socketFactory.port", "465"));
            javaMailProperties.setProperty("mail.smtp.socketFactory.class",
                    props.getProperty("mail.smtp.socketFactory.class",
                            "javax.net.ssl.SSLSocketFactory"));
        }

        javaMailProperties.setProperty("mail.smtp.auth", "true");
        javaMailProperties.setProperty("mail.debug",
                props.getProperty("mail.debug", "true"));

        message.setTo(notifConfig.getReceiver().split(","));

        if (StringUtils.isNotEmpty(notifConfig.getEmailCC())) {
            message.setCc(notifConfig.getEmailCC().split(","));
        }

        if (StringUtils.isNotEmpty(notifConfig.getEmailBCC())) {
            message.setBcc(notifConfig.getEmailBCC().split(","));
        }

        message.setSubject(notifConfig.getSubject());
        
        Map<String, Object> emailMap = new HashMap<String, Object>();
        emailMap.put("email", notifConfig);
        String text = null;
        if (StringUtils.isBlank(notifConfig.getBody())) {
            /*text = VelocityEngineUtils.mergeTemplateIntoString(velocityEngine,
                    props.getProperty("mail.template.used"),
                    Charsets.UTF_8.displayName(), emailMap);*/
            
            text = VelocityEngineUtils.mergeTemplateIntoString(velocityEngine,
                    "/templates/email-default.vm",
                    Charsets.UTF_8.displayName(), emailMap);
        } else {
            text = VelocityEngineUtils.mergeTemplateIntoString(velocityEngine,
                    notifConfig.getBody(), Charsets.UTF_8.displayName(),
                    emailMap);
        }
        System.out.println("Inside Email Service : " + notifConfig.getSubject());
        message.setText(text);

    }

}
