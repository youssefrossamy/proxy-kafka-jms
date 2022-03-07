package ma.cdgk.integration.camel.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.kafka.CloudEventDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;

import javax.json.JsonObject;


@Slf4j
@NoArgsConstructor
public class KafkaJmsProxyDeserializer implements Deserializer {

    private final ObjectMapper objectMapper = new ObjectMapper();


    @Override
    public Object deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                log.warn("Null received at deserializing");
                return null;
            } else {
                Object object = objectMapper.readValue(data, Object.class);
                if (object instanceof CloudEvent) {
                    log.info("Begin deserialization with cloud event");
                    return new CloudEventDeserializer().deserialize(topic, data);
                } else if (object instanceof Schema) {
                    log.info("Begin deserialization with avro");
                    return new KafkaAvroDeserializer().deserialize(topic, data);
                } else if (object instanceof JsonObject) {
                    log.info("Begin deserialization with json");
                    return new JsonDeserializer().deserialize(topic, data);
                } else {
                    log.warn("!!! Cannot deserialize data unknown data type, please use json , cloud event , or avro");
                    return object;
                }
            }
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing record to Object");
        }
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }
}
