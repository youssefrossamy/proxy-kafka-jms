package ma.cdgk.integration.common;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties("mongo")
@Component
@Getter
@Setter
public class MongoConfig {

    private String url;
    private String database;
    private String collection;

}