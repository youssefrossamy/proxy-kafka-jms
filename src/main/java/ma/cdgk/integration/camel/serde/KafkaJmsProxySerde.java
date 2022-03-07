package ma.cdgk.integration.camel.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaJmsProxySerde implements Serde {

    private KafkaJmsProxyDeserializer kafkaJmsProxyDeserializer;
    private KafkaJmsProxySerializer KafkaJmsProxySerializer ;

    @Override
    public Serializer serializer() {
        return KafkaJmsProxySerializer;
    }

    @Override
    public Deserializer deserializer() {
        return kafkaJmsProxyDeserializer;
    }
}
