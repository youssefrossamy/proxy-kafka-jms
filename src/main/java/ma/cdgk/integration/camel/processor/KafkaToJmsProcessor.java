package ma.cdgk.integration.camel.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.core.v1.CloudEventV1;
import lombok.extern.slf4j.Slf4j;
import ma.cdgk.integration.model.Event;
import org.apache.camel.Exchange;

import javax.jms.DeliveryMode;
@Slf4j
public class KafkaToJmsProcessor implements org.apache.camel.Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        CloudEventV1 paylod = exchange.getIn().getBody(CloudEventV1.class);
        Event event = new ObjectMapper().readValue(paylod.getData().toBytes(),Event.class);
        exchange.getMessage().setHeader("test", true);
        exchange.getMessage().setHeader("JMSDeliveryMode", DeliveryMode.PERSISTENT);
        exchange.getIn().setBody(event);
    }
}
