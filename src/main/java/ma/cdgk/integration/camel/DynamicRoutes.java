package ma.cdgk.integration.camel;

import com.mongodb.DBObject;
import ma.cdgk.integration.camel.processor.JmsToKafkaProcessor;
import ma.cdgk.integration.camel.processor.KafkaToJmsProcessor;
import ma.cdgk.integration.camel.processor.KafkaToMongoProcessor;
import ma.cdgk.integration.common.SourceDestinationConfig;
import ma.cdgk.integration.model.Event;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaManualCommit;
import org.apache.camel.converter.jaxb.JaxbDataFormat;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.util.Arrays;
import java.util.stream.Collectors;

@Component
public class DynamicRoutes extends RouteBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicRoutes.class);

    final SourceDestinationConfig sourceDestinationConfig;


    public DynamicRoutes(SourceDestinationConfig sourceDestinationConfig) {
        this.sourceDestinationConfig = sourceDestinationConfig;
    }

    @Override
    public void configure() throws JAXBException {
        routesErrorHandler();
        jmsToKafkaRoutes();
        kafkaToJmsRoutes();
        kafkaToMongoRoute();
        //TODO : add routeo to manage event strore jms to mongo
    }

    private void routesErrorHandler() {
        errorHandler(defaultErrorHandler()
                .logNewException(true)
                .onExceptionOccurred(exchange -> {
                    LOGGER.warn("Exception occured for routeId [{}] with data \n {} \n Caused by: {} : [ {} ] ",
                            exchange.getFromRouteId() , exchange.getIn().getBody(),
                            exchange.getException().getClass().getName() ,exchange.getException().getMessage());
                    Arrays.stream(exchange
                            .getException()
                            .getStackTrace())
                            .forEach(stackTraceElement -> LoggerFactory.getLogger(exchange.getFromRouteId()).error(String.valueOf(stackTraceElement)));
//                    exchange.getContext().getRouteController().stopRoute(exchange.getFromRouteId());
                })
                .loggingLevel(LoggingLevel.ERROR)
                .logStackTrace(true)
                .maximumRedeliveries(RedeliveryPolicy.NO_MAXIMUM_REDELIVERIES));
    }

    private void jmsToKafkaRoutes() throws JAXBException {
        JaxbDataFormat jaxb = getJaxbDataFormat();
        sourceDestinationConfig.getJmsToKafkaQueueTopicPairs().forEach(queueTopicPair -> {
                    from("activemq:" + queueTopicPair.getQueue())
                            .routeId("from ".concat(queueTopicPair.getQueue().concat(" to ").concat(queueTopicPair.getTopic())))
                            .transacted()
                            .log("Start reading from queue ${header.JMSDestination.name}")
                            .choice()
                            .when( simple("${header.JMSDestination.name} in '"+ getQueuesNamesByFormat("xml")+"' ")) //queueFormat
                                .description("Use this route when the headers contain a header property called test with the value true")
                                .log(LoggingLevel.INFO, "XML :: Start Processing message from queue - " + queueTopicPair.getQueue() + " - into topic -" + queueTopicPair.getTopic()+"- for data: \n ${body}")
                                .unmarshal(jaxb)
                                .removeHeaders("JMS*")
                                .process(new JmsToKafkaProcessor())
//                                // activate to test activemq resilience : tested
//                                .process(exchange -> {
//                                    if ("camelreceiver".equals(queueTopicPair.getQueue()))
//                                        throw new RuntimeException(":::::::::: addJmsToKafkaRoutes exception befor sending to " + queueTopicPair.getTopic());
//                                })
//                                .to("seda:async-transfers") async : create new thread for async
                                .to("kafka:" + queueTopicPair.getTopic())
                            .when(simple("${header.JMSDestination.name} in '"+ getQueuesNamesByFormat("json")+"' "))
                                .description("Use this route when the headers contain a header property called test with the value true")
                                .log(LoggingLevel.INFO, "JSON :: Start Processing message from queue - " + queueTopicPair.getQueue() + " - into topic -" + queueTopicPair.getTopic()+"- for data: \n ${body}")
                                .unmarshal().json(JsonLibrary.Jackson ,Event.class)
                                .removeHeaders("JMS*")
                                .process(new JmsToKafkaProcessor())
                                    // activate to test activemq resilience : tested
//                                    .process(exchange -> {
//                                        if ("camelreceiverJSON".equals(queueTopicPair.getQueue()))
//                                            throw new RuntimeException(":::::::::: addJmsToKafkaRoutes exception befor sending to " + queueTopicPair.getTopic());
//                                    })
                                .to("kafka:" + queueTopicPair.getTopic())
                            .otherwise()
                                .log(LoggingLevel.INFO, "!!!!!!!!!!!!!!! No route provided !!!!!!!!!!!!!!!")
                            .endChoice()
                            .end()
                            .log(LoggingLevel.INFO, "END Processing message from queue - " + queueTopicPair.getQueue() + " - into topic - " + queueTopicPair.getTopic()+" -");
                }
        );
    }

    private void kafkaToJmsRoutes() throws JAXBException {
        JaxbDataFormat jaxb = getJaxbDataFormat();
        sourceDestinationConfig.getKafkaToJmsQueueTopicPairs().forEach(queueTopicPair ->
                        from("kafka:" + queueTopicPair.getTopic())
                                .routeId("from ".concat(queueTopicPair.getTopic().concat(" to ").concat(queueTopicPair.getQueue())))
                                .log(LoggingLevel.INFO, "Start Processing message from topic - " + queueTopicPair.getTopic() + " - into queue -" + queueTopicPair.getQueue()+"- for data: \n ${body}")
                                .log(LoggingLevel.INFO, "topic name ${header.kafka.TOPIC}")
                                //todo: routage
                                .process(new KafkaToJmsProcessor())
                                // activate to test KAFKA resilience : tested
//                                .process(exchange -> {
//                                    if ("jmstokafka".equals(queueTopicPair.getTopic()))
//                                        throw new RuntimeException("KAFKA resilience test :::::::::: addkafkakaToJMSRoutes exception before sending to " + queueTopicPair.getTopic());
//                                })
                                .choice()
                                    .when( simple("${header.kafka.TOPIC} contains 'xml' "))  // queueFormat
                                        .log(LoggingLevel.INFO, "Start marshaling to 'xml' format ")
                                        .marshal().jaxb(jaxb.getContextPath())
                                    .endChoice()
                                    .when( simple("${header.kafka.TOPIC} contains 'Json' "))
                                        .log(LoggingLevel.INFO, "Start marshaling to 'json' format ")
                                        .marshal().json()
                                    .endChoice()
                                    .otherwise()
                                        .log(LoggingLevel.INFO, "!!!!!!!!!!!!!!! No route provided for marshal non known format !!!!!!!!!!!!!!! : " )
                                    .endChoice()
                                .end()
                                .to("activemq:" + queueTopicPair.getQueue())
                                .transacted()
                                // activate to test activemq resilience :
//                                .process(exchange -> {
//                                   if ("jmstokafka".equals(queueTopicPair.getTopic()))
//                                       throw new RuntimeException(":::::::::: addkafkakaToJMSRoutes exception before sending to " + queueTopicPair.getTopic());
//                                })
                                .process(this::commitOffsetsManually)
                                .wireTap("direct:mongo")
                                .log(LoggingLevel.INFO, "END Processing message from topic " + queueTopicPair.getTopic() + ": to queue " + queueTopicPair.getQueue())
        );
    }

    private void commitOffsetsManually(Exchange exchange) {
            KafkaManualCommit manual =
                    exchange.getIn().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
            if (manual != null) {
                LOGGER.info("manually committing the offset");
                manual.commitSync();
            }
    }

    private void kafkaToMongoRoute() throws JAXBException {
        LOGGER.info("kafka to mongo route");
              from("direct:mongo" )
                      .routeId("from kafka to event store")
                        .log(LoggingLevel.INFO, "Start Processing message from topic - ${header.kafka.TOPIC} - into event store  for data: \n ${body}")
                      .process(new KafkaToMongoProcessor())
                      // activate to test KAFKA resilience : tested
                      .process(exchange -> {
                              throw new RuntimeException("resilience test :::::::::: mongo test ");
                      })
                      .to("mongodb:http//localhost:27017?database=event_store&collection=event&operation=save")
                      .transacted()
                      // activate to test activemq resilience :
//                      .process(exchange -> {
//                            throw new RuntimeException("resilience test :::::::::: mongo test ");
//                      })
                      .end()
                    //  .process(this::commitOffsetsManually)
                      .log(LoggingLevel.INFO, "END Processing message from topic to event store");
    }

    private JaxbDataFormat getJaxbDataFormat() throws JAXBException {
        JaxbDataFormat jaxb = new JaxbDataFormat();
        JAXBContext jaxbContext = JAXBContext.newInstance(Event.class);
        jaxb.setContext(jaxbContext);
        return jaxb;
    }

    private String getQueuesNamesByFormat(String format) {
        return sourceDestinationConfig.getJmsToKafkaQueueTopicPairs()
                .stream()
                .map(queueTopicPair -> queueTopicPair.getQueue())
                .filter(s -> s.toLowerCase().contains(format.toLowerCase()))
                .collect(Collectors.joining(","));
    }
}
