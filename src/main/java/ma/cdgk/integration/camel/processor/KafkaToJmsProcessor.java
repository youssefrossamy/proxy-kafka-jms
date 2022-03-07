package ma.cdgk.integration.camel.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.core.v1.CloudEventV1;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ma.cdgk.integration.common.QueueTopicPair;
import ma.cdgk.integration.model.Event;
import org.apache.camel.Exchange;

import javax.jms.DeliveryMode;

@Slf4j
@AllArgsConstructor
public class KafkaToJmsProcessor implements org.apache.camel.Processor {

    private QueueTopicPair queueTopicPair;

    @Override
    public void process(Exchange exchange) throws Exception {
        CloudEventV1 paylod = exchange.getIn().getBody(CloudEventV1.class);
        byte[] dataAsbytes = paylod.getData().toBytes();
        Class eventClass = Class.forName("ma.cdgk.integration.model."+queueTopicPair.getTopicMappingClass());
        Object event = new ObjectMapper().readValue(dataAsbytes,eventClass);
        exchange.getMessage().setHeader("test", true);
        exchange.getMessage().setHeader("JMSDeliveryMode", DeliveryMode.PERSISTENT);
        exchange.getIn().setBody(event);
    }
}
