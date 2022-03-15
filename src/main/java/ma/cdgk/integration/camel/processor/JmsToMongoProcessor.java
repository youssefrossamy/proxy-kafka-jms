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
        queueTopicPair = Utils.getQueueTopicPairFromConfig(
                sourceDestinationConfig.getJmsToKafkaQueueTopicPairs(),
                queueTopicPair -> queueEndpoint.getDestinationName().equals(queueTopicPair.getQueue()));
        CloudEventV1 payload = exchange.getIn().getBody(CloudEventV1.class);
        Object event = Utils.deserializeCloudEventData(queueTopicPair.getTopic(), payload.getData(), schemaRegistryUrl);
        Map<String, Object> body =
                new ObjectMapper().readValue(
                        event.toString(), new TypeReference<HashMap<String, Object>>() {
                        });
        exchange.getIn().setBody(body);
    }


}
