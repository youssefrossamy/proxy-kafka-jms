package ma.cdgk.integration.camel.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import ma.cdgk.integration.camel.util.Utils;
import ma.cdgk.integration.common.QueueTopicPair;
import ma.cdgk.integration.common.SourceDestinationConfig;
import ma.cdgk.integration.normalizer.EventNormalizer;
import org.apache.camel.Exchange;
import org.apache.camel.component.activemq.ActiveMQQueueEndpoint;
import org.springframework.context.ApplicationContext;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.UUID;


public class JmsToKafkaProcessor implements org.apache.camel.Processor  {

    public static final String APPLICATION_JSON = "application/json";
    private SourceDestinationConfig sourceDestinationConfig ;
    private ApplicationContext applicationContext ;
    private QueueTopicPair queueTopicPair;
    private String schemaRegistryUrl;

    public JmsToKafkaProcessor(SourceDestinationConfig sourceDestinationConfig, ApplicationContext applicationContext ,String schemaRegistryUrl ) {
        this.sourceDestinationConfig = sourceDestinationConfig;
        this.applicationContext = applicationContext;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    QueueTopicPair getQueueTopicPairFromQueName(String queueName){
        return sourceDestinationConfig.getJmsToKafkaQueueTopicPairs()
                .stream()
                .filter(queueTopicPair -> queueName.equals(queueTopicPair.getQueue()))
                .findFirst().orElse(null);
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        ActiveMQQueueEndpoint queueEndpoint = (ActiveMQQueueEndpoint) exchange.getFromEndpoint();
        queueTopicPair = getQueueTopicPairFromQueName(queueEndpoint.getDestinationName());
        Class clazz = Class.forName(queueTopicPair.getQueueMappingClass());
//        Object body = exchange.getIn().getBody(clazz);
        Object body = new ObjectMapper().convertValue(exchange.getIn().getBody() , clazz);
        if ("CloudEvent".equals(queueTopicPair.getTopicFormat())) {
            EventNormalizer normalizer = getNormaliser();
            Object object = normalizer.normalize(body);
            BytesCloudEventData bytesCloudEventData = BytesCloudEventData
                    .wrap(
                            Utils.serializeCloudEventData(object , schemaRegistryUrl , queueTopicPair.getTopic()));
            exchange.getMessage().setHeader("content-type", "application/avro" );
            CloudEvent event = CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())//RANDOM ID
                    .withType(object.getClass().getName()) //should not be Object ?? queueTopicPair.getQueueMappingClass()
                    .withSource(URI.create("http://localhost"))
                    .withTime(OffsetDateTime.now())
                    .withDataContentType("application/avro") // "application/avro"
                    .withData(bytesCloudEventData)
                    .build();
            exchange.getIn().setBody(event);
        }
    }

    private EventNormalizer getNormaliser() throws ClassNotFoundException {
        return (EventNormalizer) applicationContext.getBean(Class.forName(queueTopicPair.getNormalizer()));
    }

}
