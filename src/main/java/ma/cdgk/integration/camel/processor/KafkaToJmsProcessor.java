package ma.cdgk.integration.camel.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.core.v1.CloudEventV1;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ma.cdgk.domain.events.bancaire.AmortissementCreditEvent;
import ma.cdgk.integration.camel.mapping.EventMapping;
import ma.cdgk.integration.camel.util.Utils;
import ma.cdgk.integration.common.QueueTopicPair;
import ma.cdgk.integration.common.SourceDestinationConfig;
import org.apache.camel.Exchange;
import org.apache.camel.model.dataformat.AvroLibrary;
import org.springframework.context.ApplicationContext;

import javax.jms.DeliveryMode;

@Slf4j
@AllArgsConstructor
public class KafkaToJmsProcessor implements org.apache.camel.Processor {

    private ApplicationContext applicationContext;
    private SourceDestinationConfig sourceDestinationConfig;
    private QueueTopicPair queueTopicPair;
    private String schemaRegistryUrl;

    public KafkaToJmsProcessor(ApplicationContext applicationContext, SourceDestinationConfig sourceDestinationConfig, String schemaRegistryUrl) {
        this.applicationContext = applicationContext;
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
        Class eventClass = Class.forName(queueTopicPair.getTopicMappingClass());
        Object event = new ObjectMapper().readValue(
                Utils.deserializeCloudEventData(queueTopicPair.getTopic(), paylod.getData(), schemaRegistryUrl).toString()
                , eventClass);
        EventMapping<Object, Object> eventMapping ;
        if (Utils.XML_FORMAT.equals(queueTopicPair.getQueueFormat())) {
            eventMapping = getXmlMapper(queueTopicPair.getXmlMapper());
            event = eventMapping.map(event);
//        } else  if (Utils.JSON_FORMAT.equals(queueTopicPair.getQueueFormat())) {
//            eventMapping =getJsonMapper(queueTopicPair.getJsonMapper());
//            event = eventMapping.map(event);
        }
        exchange.getMessage().setHeader("JMSDeliveryMode", DeliveryMode.PERSISTENT);
        exchange.getIn().setBody(event);
    }


    private EventMapping getXmlMapper(String mappingClassName) throws ClassNotFoundException {
        return (EventMapping) applicationContext.getBean(Class.forName(mappingClassName));
    }

    private EventMapping getJsonMapper(String mappingClassName) throws ClassNotFoundException {
        return (EventMapping) applicationContext.getBean(Class.forName(mappingClassName));
    }


}
