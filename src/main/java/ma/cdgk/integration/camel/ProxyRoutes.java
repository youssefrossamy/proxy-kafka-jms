package ma.cdgk.integration.camel;

import ma.cdgk.integration.camel.processor.JmsToKafkaProcessor;
import ma.cdgk.integration.camel.util.Utils;
import ma.cdgk.integration.common.MongoConfig;
import ma.cdgk.integration.common.QueueTopicPair;
import ma.cdgk.integration.common.SourceDestinationConfig;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaManualCommit;
import org.apache.camel.converter.jaxb.JaxbDataFormat;
import org.apache.camel.model.dataformat.AvroDataFormat;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.util.Arrays;

@Component
public class ProxyRoutes extends RouteBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyRoutes.class);

    final SourceDestinationConfig sourceDestinationConfig;
    final MongoConfig mongoConfig;
    JmsToKafkaProcessor jmsToKafkaProcessor;

    public ProxyRoutes(SourceDestinationConfig sourceDestinationConfig , MongoConfig mongoConfig) {
        this.sourceDestinationConfig = sourceDestinationConfig;
        this.mongoConfig =mongoConfig;
    }

    @Override
    public void configure() {
        routesErrorHandler();
        jmsToKafkaRoutes();
        jmsToMongoRoute();
        kafkaToJmsRoutes();
        kafkaToMongoRoute();
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

    private void jmsToKafkaRoutes(){
        sourceDestinationConfig.getJmsToKafkaQueueTopicPairs()
                .forEach(queueTopicPair ->
                    from("activemq:" + queueTopicPair.getQueue())
                            .routeId("from ".concat(queueTopicPair.getQueue().concat(" to ").concat(queueTopicPair.getTopic())))
                            .transacted()
                            .log("Start reading from queue ${header.JMSDestination.name}")
                            .removeHeaders("JMS*")
                            .choice()
                            .when( exchange -> Utils.XML_FORMAT.equals(queueTopicPair.getQueueFormat()))  // queueFormat
                                .description("Use this route when the headers contain a header property called test with the value true")
                                .log(LoggingLevel.INFO, "XML :: Start Processing message from queue - " + queueTopicPair.getQueue() + " - into topic -" + queueTopicPair.getTopic()+"- for data: \n ${body}")
                                .unmarshal(getJaxbDataFormat(queueTopicPair))
                                .process("jmsToKafkaProcessor")
//                                // activate to test activemq resilience : tested
//                                .process(exchange -> {
//                                    if ("camelreceiver".equals(queueTopicPair.getQueue()))
//                                        throw new RuntimeException(":::::::::: addJmsToKafkaRoutes exception befor sending to " + queueTopicPair.getTopic());
//                                })
                            .to("kafka:" + queueTopicPair.getTopic())
                            .endChoice()
                            .when( exchange -> Utils.JSON_FORMAT.equals(queueTopicPair.getQueueFormat()))  // queueFormat
                                .description("Use this route when the headers contain a header property called test with the value true")
                                .log(LoggingLevel.INFO, "JSON :: Start Processing message from queue - " + queueTopicPair.getQueue() + " - into topic - " + queueTopicPair.getTopic()+" - for data: \n ${body}")
                                .unmarshal().json(JsonLibrary.Jackson ,Object.class)
                                .process("jmsToKafkaProcessor")
                                    // activate to test activemq resilience : tested
//                                    .process(exchange -> {
//                                        if ("camelreceiverJSON".equals(queueTopicPair.getQueue()))
//                                            throw new RuntimeException(":::::::::: addJmsToKafkaRoutes exception befor sending to " + queueTopicPair.getTopic());
//                                    })
                            .to("kafka:" + queueTopicPair.getTopic())
                            .endChoice()
                            .otherwise()
                                .log(LoggingLevel.INFO, "!!!!!!!!!!!!!!!choice provided for this "+queueTopicPair.getQueueFormat()+" data format !!!!!!!!!!!!!!!")
                            .endChoice()
                            .end()
                            //info: works well
                             .wireTap(Boolean.parseBoolean(queueTopicPair.getMongoJournaly())?"direct:JmsToMongo":"log:JmsToMongo")
                            .log(LoggingLevel.INFO, "END Processing message from queue - " + queueTopicPair.getQueue() + " - into topic - " + queueTopicPair.getTopic()+" -")
        );
    }

    private void kafkaToJmsRoutes() {
        AvroDataFormat avroDataFormat = new AvroDataFormat();
        avroDataFormat.getDataFormat();
        sourceDestinationConfig.getKafkaToJmsQueueTopicPairs().forEach(queueTopicPair ->
                        from("kafka:" + queueTopicPair.getTopic())
                                .routeId("from ".concat(queueTopicPair.getTopic().concat(" to ").concat(queueTopicPair.getQueue())))
                                .log(LoggingLevel.INFO, "Start Processing message from topic - " + queueTopicPair.getTopic() + " - into queue - " + queueTopicPair.getQueue()+" - for data: \n ${body}")
                                .log(LoggingLevel.INFO, "topic name ${header.kafka.TOPIC}")
                                .process("kafkaToJmsProcessor")
                                // activate to test KAFKA resilience : tested
//                                .process(exchange -> {
//                                    if ("jmstokafka".equals(queueTopicPair.getTopic()))
//                                        throw new RuntimeException("KAFKA resilience test :::::::::: addkafkakaToJMSRoutes exception before sending to " + queueTopicPair.getTopic());
//                                })
                                .choice()
                                    .when( exchange -> Utils.XML_FORMAT.equals(queueTopicPair.getQueueFormat()))  // queueFormat
                                        .log(LoggingLevel.INFO, "kafkaToJmsRoutes : Start marshaling to 'xml' format")
                                        .marshal().jaxb(getJaxbDataFormat(queueTopicPair).getContextPath())
                                    .endChoice()
                                    .when(exchange -> Utils.JSON_FORMAT.equals(queueTopicPair.getQueueFormat()))
                                        .log(LoggingLevel.INFO, "kafkaToJmsRoutes : Start marshaling to 'json' format")
                                        .marshal(avroDataFormat)
                                    .endChoice()
                                    .otherwise()
                                        .log(LoggingLevel.INFO, "!!!!!!!!!!!!!!! No case provided for marshal unknown format "+queueTopicPair.getQueueFormat()+"!!!!!!!!!!!!!!! : " )
                                    .endChoice()
                                .end()
                                .removeHeaders("KAFKA*")
                                .to("activemq:" + queueTopicPair.getQueue())
                                .transacted()
                                // activate to test activemq resilience :
//                                .process(exchange -> {
//                                   if ("jmstokafka".equals(queueTopicPair.getTopic()))
//                                       throw new RuntimeException(":::::::::: addkafkakaToJMSRoutes exception before sending to " + queueTopicPair.getTopic());
//                                })
                                .process(this::commitOffsetsManually)
                                    .wireTap(Boolean.parseBoolean(queueTopicPair.getMongoJournaly())?"direct:kafkaToMongo":"log:kafkaToMongo")
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

    private void kafkaToMongoRoute(){
        LOGGER.info("kafka to mongo route");
              from("direct:kafkaToMongo" )
                      .routeId("From kafka to event store")
                        .log(LoggingLevel.INFO, "Journalization: Start Processing message into event store  for data: \n ${body}")
                      .process("kafkaToMongoProcessor")
                      // activate to test KAFKA resilience :
//                      .process(exchange -> {
//                              throw new RuntimeException("resilience test :::::::::: mongo test ");
//                      })
                      .to("mongodb:"+ mongoConfig.getUrl()+"?database="+ mongoConfig.getDatabase()+"&collection="+ mongoConfig.getCollection()+"&operation=save")
                      .transacted()
                      // activate to test activemq resilience :
//                      .process(exchange -> {
//                            throw new RuntimeException("resilience test :::::::::: mongo test ");
//                      })
                      .end()
                      .log(LoggingLevel.INFO, "END Processing message from topic to event store");
    }

    private void jmsToMongoRoute(){
        LOGGER.info("JMS to mongo route");
              from("direct:JmsToMongo" )
                      .routeId("from Jms to event store")
                        .log(LoggingLevel.INFO, "Journalization: Start Processing message into event store for data: \n ${body}")
                      .process("jmsToMongoProcessor")
                      // activate to test resilience : tested
//                      .process(exchange -> {
//                              throw new RuntimeException("resilience test :::::::::: mongo test ");
//                      })
                      .to("mongodb:"+ mongoConfig.getUrl()+"?database="+ mongoConfig.getDatabase()+"&collection="+ mongoConfig.getCollection()+"&operation=save")
                      //.to("mongodb:http//localhost:27017?database=event_store&collection=event&operation=save")
                      .transacted()
                      // activate to test activemq resilience :
//                      .process(exchange -> {
//                            throw new RuntimeException("resilience test :::::::::: mongo test ");
//                      })
                      .end()
                      .log(LoggingLevel.INFO, "END Processing message from queue to event store");
    }

    private JaxbDataFormat getJaxbDataFormat(QueueTopicPair queueTopicPair)  {
        JAXBContext jaxbContext = null;
        JaxbDataFormat jaxb = new JaxbDataFormat();
        try {
            Class<?> aClass = Class.forName(queueTopicPair.getQueueMappingClass());
            jaxbContext = JAXBContext.newInstance(aClass);
            jaxb.setContext(jaxbContext);
        } catch (ClassNotFoundException | JAXBException e) {
            e.printStackTrace();
        }
        return jaxb;
    }

}
