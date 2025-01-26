package de.quoss.mq.client;

import com.ibm.mq.jms.MQQueueConnection;
import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import java.util.Enumeration;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;

public class App {

    public static void main(final String[] args) {
        new App().run();
    }

    private void run() {
        final MQQueueConnectionFactory factory = new MQQueueConnectionFactory();
        try {
            factory.setQueueManager("QM1");
            factory.setChannel("DEV.APP.SVRCONN");
            factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
        } catch (final JMSException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        try (final QueueConnection connection = factory.createQueueConnection("app", "passw0rd");
                final QueueSession session = connection.createQueueSession(true, Session.AUTO_ACKNOWLEDGE)) {
            System.out.println("run [connection=%s,session=%s]".formatted(connection, session));
            final Queue queue = session.createQueue("DEV.QUEUE.1");
            final QueueBrowser browser = session.createBrowser(queue);
            final Enumeration<?> enumeration = browser.getEnumeration();
            int i = 0;
            while (enumeration.hasMoreElements()) {
                final Object o = enumeration.nextElement();
                System.out.println("run [o=%s]".formatted(o));
                i++;
            }
            System.out.println("run [i=%s]".formatted(i));
            browser.close();
        } catch (final JMSException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

}
