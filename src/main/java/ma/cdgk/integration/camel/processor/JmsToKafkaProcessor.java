package ma.cdgk.integration.camel.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import ma.cdgk.integration.common.QueueTopicPair;
import ma.cdgk.integration.common.SourceDestinationConfig;
import ma.cdgk.integration.eventNormalizer.EventNormalizer;
import org.apache.camel.Exchange;
import org.apache.camel.component.activemq.ActiveMQQueueEndpoint;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
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

        Object body = exchange.getIn().getBody(clazz);

        if ("CloudEvent".equals(queueTopicPair.getTopicFormat())) {
            ObjectMapper objectMapper = new ObjectMapper();
            EventNormalizer normalizer = getNormaliser();
            Object object = normalizer.normalize(body);
            //Call Normalizer
            BytesCloudEventData bytesCloudEventData = BytesCloudEventData.wrap(serializeData(object));
//            CloudEventData cloudEventData = BytesCloudEventData.wrap(objectMapper.writeValueAsBytes(object));
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

    private byte[] serializeData(Object item) {
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        config.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer();
        avroSerializer.configure(config, false);
        return avroSerializer.serialize(queueTopicPair.getTopic(), item);
    }
}
