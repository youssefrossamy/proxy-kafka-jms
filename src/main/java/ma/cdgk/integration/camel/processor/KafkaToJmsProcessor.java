package ma.cdgk.integration.camel.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.core.v1.CloudEventV1;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ma.cdgk.integration.camel.mapping.EventMapping;
import ma.cdgk.integration.camel.util.Utils;
import ma.cdgk.integration.common.QueueTopicPair;
import ma.cdgk.integration.common.SourceDestinationConfig;
import org.apache.camel.Exchange;
import org.springframework.context.ApplicationContext;
import org.springframework.util.ObjectUtils;

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
        String topicName = (String) exchange.getIn().getHeader(Utils.KAFKA_TOPIC_HEADER_NAME);
        queueTopicPair = Utils.getQueueTopicPairFromConfig(
                sourceDestinationConfig.getKafkaToJmsQueueTopicPairs(),
                qtp -> topicName.equals(qtp.getTopic()));
        Class<?> eventClass = Class.forName(queueTopicPair.getTopicMappingClass());
        Object event = new ObjectMapper().readValue(
                Utils.deserializeCloudEventData(queueTopicPair.getTopic(), paylod.getData(), schemaRegistryUrl).toString()
                , eventClass);
        EventMapping<Object, Object> eventMapping ;
        String mapper = getMapperName(queueTopicPair);
        if(!ObjectUtils.isEmpty(mapper)){
            eventMapping = getMapper(mapper);
            event = eventMapping.map(event);
        }
        exchange.getMessage().setHeader(Utils.JMS_DELIVERY_MODE, DeliveryMode.PERSISTENT);
        exchange.getIn().setBody(event);
    }


    String getMapperName(QueueTopicPair queueTopicPair){
        if (Utils.XML_FORMAT.equals(queueTopicPair.getQueueFormat())) {
            return queueTopicPair.getXmlMapper();
//TODO: activate when an implementation of mapper to json object is provided
/*        } else  if (Utils.JSON_FORMAT.equals(queueTopicPair.getQueueFormat())) {
            return queueTopicPair.getJsonMapper();*/
        }
        return null;
    }

    private EventMapping<Object,Object> getMapper(String mappingClassName) throws ClassNotFoundException {
        return (EventMapping) applicationContext.getBean(Class.forName(mappingClassName));
    }


}
