package de.quoss.mq.client;

import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;

import java.util.Arrays;
import java.util.Enumeration;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {

    public static final Logger LOGGER = LoggerFactory.getLogger(App.class);
    
    final String channelValue;
    final String functionValue;
    final String hostNameValue;
    final String passwordValue;
    final String portValue;
    final String queueValue;
    final String queueManagerValue;
    final String typeValue;
    final String useridValue;
    
    public App(final String[] args) {
        LOGGER.trace("init start [args={}]", Arrays.asList(args));
        final Options options = new Options();
        final Option channelOption = Option.builder("c")
                .longOpt("channel")
                .hasArg()
                .required()
                .build();
        final Option functionOption = Option.builder("fn")
                .longOpt("function")
                .hasArg()
                .required()
                .build();
        final Option hostNameOption = Option.builder("h")
                .longOpt("host-name")
                .hasArg()
                .build();
        final Option passwordOption = Option.builder("pw")
                .longOpt("password")
                .hasArg()
                .required()
                .build();
        final Option portOption = Option.builder("p")
                .longOpt("port")
                .hasArg()
                .build();
        final Option queueOption = Option.builder("q")
                .longOpt("queue")
                .hasArg()
                .required()
                .build();
        final Option queueManagerOption = Option.builder("qm")
                .longOpt("queue-manager")
                .hasArg()
                .required()
                .build();
        final Option typeOption = Option.builder("t")
                .longOpt("type")
                .hasArg()
                .required()
                .build();
        final Option useridOption = Option.builder("u")
                .longOpt("userid")
                .hasArg()
                .required()
                .build();
        options.addOption(channelOption);
        options.addOption(functionOption);
        options.addOption(hostNameOption);
        options.addOption(passwordOption);
        options.addOption(portOption);
        options.addOption(queueOption);
        options.addOption(queueManagerOption);
        options.addOption(typeOption);
        options.addOption(useridOption);
        final CommandLineParser parser = new DefaultParser();
        final CommandLine commandLine;
        try {
            commandLine = parser.parse(options, args);
        } catch (final ParseException e) {
            throw new MqClientException("Error parsing command line: " + e.getMessage(), e);
        }
        channelValue = commandLine.getOptionValue("c");
        functionValue = commandLine.getOptionValue("fn");
        hostNameValue = commandLine.getOptionValue("h");
        passwordValue = commandLine.getOptionValue("pw");
        portValue = commandLine.getOptionValue("p");
        queueValue = commandLine.getOptionValue("q");
        queueManagerValue = commandLine.getOptionValue("qm");
        typeValue = commandLine.getOptionValue("t");
        useridValue = commandLine.getOptionValue("u");
        LOGGER.trace("init end");
    }
    
    public static void main(final String[] args) {
        LOGGER.trace("main start");
        new App(args).run();
        LOGGER.trace("main end");
    }

    private Connection connection(final ConnectionFactory factory) throws JMSException {
        LOGGER.trace("connection start");
        final Connection result;
        if (passwordValue == null || useridValue == null) {
            throw new MqClientException("Invalid credentials provided");
        }
        result = factory.createConnection(useridValue, passwordValue);
        LOGGER.trace("connection end [result={}]", result);
        return result;
    }

    private ConnectionFactory connectionFactory() {
        LOGGER.trace("connection-factory start");
        final MQQueueConnectionFactory result = new MQQueueConnectionFactory();
        try {
            result.setChannel(channelValue);
            if (hostNameValue != null) {
                result.setHostName(hostNameValue);
            }
            if (portValue != null) {
                result.setPort(Integer.parseInt(portValue));
            }
            result.setQueueManager(queueManagerValue);
            result.setTransportType(WMQConstants.WMQ_CM_CLIENT);
        } catch (final JMSException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        LOGGER.trace("connection-factory end [result={}]", result);
        return result;
    }

    private void listMessageIds() {
        LOGGER.trace("list-message-ids start");
        if (null == typeValue) {
            throw new MqClientException("No type value provided");
        } else {
            switch (typeValue) {
                case "jms" -> listMessageIdsJms();
                default -> throw new MqClientException("Type value '" + typeValue + "' not supported");
            }
        }
        LOGGER.trace("list-message-ids end");
    }

    private void listMessageIdsJms() {
        LOGGER.trace("list-message-ids-jms start");
        final ConnectionFactory factory = connectionFactory();
        try (final Connection connection = connection(factory);
             final Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE)) {
            LOGGER.debug("list-message-ids-jms [connection={},session={}]", connection, session);
            final Queue queue = session.createQueue(queueValue);
            final QueueBrowser browser = session.createBrowser(queue);
            final Enumeration<?> enumeration = browser.getEnumeration();
            int i = 0;
            while (enumeration.hasMoreElements()) {
                final Object o = enumeration.nextElement();
                LOGGER.debug("list-message-ids-jms [o.class.name={},o={}]", o.getClass().getName(), o);
                i++;
            }
            LOGGER.debug("list-message-ids-jms [i={}]", i);
            browser.close();
        } catch (final JMSException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        LOGGER.trace("list-message-ids-jms end");
    }

    private void putMessage() {
        LOGGER.trace("put-message start");
        if (null == typeValue) {
            throw new MqClientException("No type value provided");
        } else {
            switch (typeValue) {
                case "jms" -> putMessageJms();
                default -> throw new MqClientException("Type value '" + typeValue + "' not supported");
            }
        }
        LOGGER.trace("put-message end");
    }

    private void putMessageJms() {
        LOGGER.trace("put-message-jms start");
        final ConnectionFactory factory = connectionFactory();
        try (final Connection connection = connection(factory);
             final Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE)) {
            LOGGER.debug("put-message-jms [connection={},session={}]", connection, session);
            final Queue queue = session.createQueue(queueValue);
            final TextMessage message = session.createTextMessage();
            message.setText("foo");
            try (final MessageProducer producer = session.createProducer(queue)) {
                producer.send(message);
            }
            session.commit();
        } catch (final JMSException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        LOGGER.trace("put-message-jms end");
    }

    private void run() {
        LOGGER.trace("run start");
        if (null == functionValue) {
            throw new MqClientException("No function value provided");
        } else {
            switch (functionValue) {
                case "list-message-ids" -> listMessageIds();
                case "put-message" -> putMessage();
                default -> throw new MqClientException("Function value '" + functionValue + "' not supported");
            }
        }
        LOGGER.trace("run end");
    }

}
