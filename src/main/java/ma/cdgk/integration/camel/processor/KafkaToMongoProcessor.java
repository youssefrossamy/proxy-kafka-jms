package ma.cdgk.integration.camel.processor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.core.v1.CloudEventV1;
import lombok.extern.slf4j.Slf4j;
import ma.cdgk.integration.camel.util.Utils;
import ma.cdgk.integration.common.QueueTopicPair;
import ma.cdgk.integration.common.SourceDestinationConfig;
import org.apache.camel.Exchange;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class KafkaToMongoProcessor implements org.apache.camel.Processor {

    private SourceDestinationConfig sourceDestinationConfig ;
    private QueueTopicPair queueTopicPair;
    private String schemaRegistryUrl;

    public KafkaToMongoProcessor(SourceDestinationConfig sourceDestinationConfig, String schemaRegistryUrl) {
        this.sourceDestinationConfig = sourceDestinationConfig;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        CloudEventV1 paylod = exchange.getIn().getBody(CloudEventV1.class);
        String topicName = (String) exchange.getIn().getHeader("kafka.TOPIC");
        queueTopicPair = Utils.getQueueTopicPairFromConfig(
                sourceDestinationConfig.getKafkaToJmsQueueTopicPairs(),
                queueTopicPair -> topicName.equals(queueTopicPair.getTopic()));
        Object event = Utils.deserializeCloudEventData(queueTopicPair.getTopic(),paylod.getData(),schemaRegistryUrl);
//        Class clazz = Class.forName(queueTopicPair.getQueueMappingClass());
//        Object payload = new ObjectMapper().convertValue(event , clazz);
        Map<String, Object> payload =
                new ObjectMapper().readValue(
                        event.toString(), new TypeReference<HashMap<String, Object>>() {});
        exchange.getIn().setBody(payload);
    }
}
