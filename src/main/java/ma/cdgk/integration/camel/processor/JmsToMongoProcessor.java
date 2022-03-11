package ma.cdgk.integration.camel.processor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.core.v1.CloudEventV1;
import lombok.extern.slf4j.Slf4j;
import ma.cdgk.integration.camel.util.Utils;
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
        Object event = Utils.deserializeCloudEventData(queueTopicPair.getTopic(),paylod.getData(),schemaRegistryUrl);
        Map<String, Object> payload =
                new ObjectMapper().readValue(
                        event.toString(), new TypeReference<HashMap<String, Object>>() {});
        exchange.getIn().setBody(payload);
    }

    QueueTopicPair getQueueTopicPairFromQueName(String queueName){
        return sourceDestinationConfig.getJmsToKafkaQueueTopicPairs()
                .stream()
                .filter(queueTopicPair -> queueName.equals(queueTopicPair.getQueue()))
                .findFirst().orElse(null);
    }

}
