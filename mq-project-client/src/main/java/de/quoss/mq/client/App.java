package de.quoss.mq.client;

import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import java.util.Enumeration;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
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
    }
    
    public static void main(final String[] args) {
        LOGGER.trace("main start");
        new App(args).run();
        LOGGER.trace("main end");
    }

    private void listMessageIds() {
        if (null == typeValue) {
            throw new MqClientException("No type value provided");
        } else {
            switch (typeValue) {
                case "jms" -> listMessageIdsJms();
                default -> throw new MqClientException("Type value '" + typeValue + "' not supported");
            }
        }
    }

    private void listMessageIdsJms() {
        final MQQueueConnectionFactory factory = new MQQueueConnectionFactory();
        try {
            factory.setChannel(channelValue);
            if (hostNameValue != null) {
                factory.setHostName(hostNameValue);
            }
            if (portValue != null) {
                factory.setPort(Integer.parseInt(portValue));
            }
            factory.setQueueManager(queueManagerValue);
            factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
        } catch (final JMSException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        if (passwordValue == null || useridValue == null) {
            throw new MqClientException("Invalid credentials provided");
        }
        try (final QueueConnection connection = factory.createQueueConnection(useridValue, passwordValue);
                final QueueSession session = connection.createQueueSession(true, Session.AUTO_ACKNOWLEDGE)) {
            LOGGER.trace("list-message-ids-jms [connection={},session={}]", connection, session);
            final Queue queue = session.createQueue(queueValue);
            final QueueBrowser browser = session.createBrowser(queue);
            final Enumeration<?> enumeration = browser.getEnumeration();
            int i = 0;
            while (enumeration.hasMoreElements()) {
                final Object o = enumeration.nextElement();
                LOGGER.trace("list-message-ids-jms [o={}]", o);
                i++;
            }
            LOGGER.trace("list-message-ids-jms [i={}]", i);
            browser.close();
        } catch (final JMSException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
    
    private void run() {
        if (null == functionValue) {
            throw new MqClientException("No function value provided");
        } else {
            switch (functionValue) {
                case "list-message-ids" -> listMessageIds();
                default -> throw new MqClientException("Function value '" + functionValue + "' not supported");
            }
        }
    }

}
