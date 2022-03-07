package ma.cdgk.integration.camel.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEventData;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.core.v1.CloudEventV1;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.extern.slf4j.Slf4j;
import ma.cdgk.integration.common.QueueTopicPair;
import ma.cdgk.integration.common.SourceDestinationConfig;
import org.apache.camel.Exchange;
import org.apache.camel.component.activemq.ActiveMQQueueEndpoint;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class JmsToMongoProcessor implements org.apache.camel.Processor {
    private SourceDestinationConfig sourceDestinationConfig;
    private String schemaRegistryUrl;
    private QueueTopicPair queueTopicPair;

    public JmsToMongoProcessor(SourceDestinationConfig sourceDestinationConfig, String schemaRegistryUrl) {
        this.sourceDestinationConfig = sourceDestinationConfig;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        ActiveMQQueueEndpoint queueEndpoint = (ActiveMQQueueEndpoint) exchange.getFromEndpoint();
        queueTopicPair = getQueueTopicPairFromQueName(queueEndpoint.getDestinationName());
        CloudEventV1 paylod = exchange.getIn().getBody(CloudEventV1.class);
//        Object event = new ObjectMapper().readValue(paylod.getData().toBytes(), Object.class);
        Object event = deserializeCloudEventData(queueTopicPair.getTopic(),paylod.getData());
        exchange.getIn().setBody(new ObjectMapper().convertValue(event., Map.class));//todo: correct exception here
    }

    QueueTopicPair getQueueTopicPairFromQueName(String queueName){
        return sourceDestinationConfig.getJmsToKafkaQueueTopicPairs()
                .stream()
                .filter(queueTopicPair -> queueName.equals(queueTopicPair.getQueue()))
                .findFirst().orElse(null);
    }
    private Object deserializeCloudEventData(String topic, CloudEventData cloudEventData) {
        BytesCloudEventData bytesCloudEventData = (BytesCloudEventData) cloudEventData;
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        config.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        KafkaAvroDeserializer avroDeserializer = new KafkaAvroDeserializer();
        avroDeserializer.configure(config, false);
        return avroDeserializer.deserialize(topic, bytesCloudEventData.toBytes());
    }
}
