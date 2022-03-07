package ma.cdgk.integration.camel.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventV1;
import io.cloudevents.kafka.CloudEventSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

import javax.json.JsonObject;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;


@Slf4j
@NoArgsConstructor
public class KafkaJmsProxySerializer implements Serializer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Object data) {
        try {
            if (data == null) {
                log.warn("Null received at serializing");
                return null;
            } else {
                if (data instanceof CloudEvent) {
                    CloudEvent cloudEventV1 = (CloudEvent) data;
                    RecordHeaders  headers = new RecordHeaders();
                    headers.add("specversion",Optional.ofNullable(cloudEventV1.getSpecVersion().toString()).orElse("").getBytes(StandardCharsets.UTF_8));
                    headers.add("id",Optional.ofNullable(cloudEventV1.getId()).orElse("").getBytes());
                    headers.add("subject", Optional.ofNullable(cloudEventV1.getSubject()).orElse("").getBytes());
                    headers.add("type",Optional.ofNullable(cloudEventV1.getType()).orElse("").getBytes());
                    headers.add("datacontenttype",Optional.ofNullable(cloudEventV1.getDataContentType()).orElse("").getBytes());
                    headers.add("dataschema",Optional.ofNullable(cloudEventV1.getDataSchema()).orElse(new URI("")).toString().getBytes(StandardCharsets.UTF_8));
                    headers.add("time",Optional.ofNullable(cloudEventV1.getTime()).orElse(LocalDateTime.now().atOffset(ZoneOffset.UTC)).toString().getBytes(StandardCharsets.UTF_8));
                    log.info("Begin serialization with cloud event");
                    return new CloudEventSerializer().serialize(topic,headers, cloudEventV1);
                } else if (data instanceof Schema) {
                    log.info("Begin serialization with avro");
                    return new KafkaAvroSerializer().serialize(topic, data);
                } else if (data instanceof JsonObject) {
                    log.info("Begin serialization with json");
                    return new JsonSerializer().serialize(topic, data);
                } else {
                    log.warn("!!! Cannot serialize data unknown data type, please use json , cloud event , or avro");
                    return objectMapper.writeValueAsBytes(data);
                }
            }
        } catch (Exception e) {
            throw new SerializationException("Error when serializing record to byte[]");
        }
    }


}
