//package dataCollection;
import java.io.IOException;

import java.util.Date;
import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Message;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class Mail{ 
	
    String smtpHost = "";
    String smtpPort = "";
    String gmailSender = "";
    String gmailPwd = "";
    String gmailRecvr = "";
	
    Mail(){
	// default constructor
    }
    
    Mail(String sender, String recvr, String pwd){
	
	this.smtpHost = "smtp.gmail.com";
        this.smtpPort = "587";
        this.gmailSender = sender;
        this.gmailPwd = pwd;
        this.gmailRecvr = recvr;
    }

    public <E> void send(E msgTxt) {
    		
        Properties prop = System.getProperties();
        prop.setProperty("mail.smtp.auth", "true");
        prop.setProperty("mail.smtp.starttls.enable", "true");
        prop.setProperty("mail.smtp.host", this.smtpHost);
        prop.setProperty("mail.smtp.port", this.smtpPort);
        
        Session session = Session.getInstance(prop, new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(gmailSender, gmailPwd);
            }
        });

        Message message = new MimeMessage(session);
        try {
            message.setFrom(new InternetAddress(this.gmailSender));
            message.setRecipients(Message.RecipientType.TO,InternetAddress.parse(this.gmailRecvr));
            message.setSubject("Test RSSServer still running");
            message.setText((String)msgTxt);
            Transport.send(message);
        } catch (AddressException e) {
            e.printStackTrace();
        } catch (MessagingException e) {
            System.out.println((new Date()).toString() + " Message exception: " + e.getMessage());
            //e.printStackTrace();
        }
    }   
    
    public void test() {

	try{
            this.send("test message"); 
        }catch(Exception e){
            System.out.println("Error sending Mail " + e.getMessage());
        System.exit(-1);
        }
    }
	
    public static void main(String[] args) throws IOException{
		 
	Mail m = new Mail("senderTest@gmail.com", "recvrTest@gmail.com", "mypassword12345");
	m.test();
    }
	 
}
