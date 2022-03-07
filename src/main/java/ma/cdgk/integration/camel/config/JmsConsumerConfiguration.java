package ma.cdgk.integration.camel.config;

import ma.cdgk.integration.camel.processor.JmsToKafkaProcessor;
import ma.cdgk.integration.camel.processor.JmsToMongoProcessor;
import ma.cdgk.integration.common.SourceDestinationConfig;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.camel.Processor;
import org.apache.camel.component.kafka.springboot.KafkaComponentConfiguration;
import org.checkerframework.checker.units.qual.A;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.connection.JmsTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableJms
public class JmsConsumerConfiguration {

    @Value("${artemis.broker-url}")
    private String brokerUrl;

    @Value("${artemis.broker-username}")
    private String brokerUsename;

    @Value("${artemis.broker-password}")
    private String brokerPassword;

    @Value("${camel.component.kafka.schema-registry-u-r-l}")
    private String schemaRegistryUrl;

    @Autowired
    private SourceDestinationConfig sourceDestinationConfig ;

    @Autowired
    private ApplicationContext applicationContext ;

    @Bean
    public ActiveMQConnectionFactory receiverActiveMQConnectionFactory() {
        return new ActiveMQConnectionFactory(brokerUrl, brokerUsename, brokerPassword);
    }

    @Bean
    public DefaultJmsListenerContainerFactory jmsListenerContainerFactory() {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setSessionAcknowledgeMode(javax.jms.Session.CLIENT_ACKNOWLEDGE);
        factory.setConnectionFactory(receiverActiveMQConnectionFactory());
        return factory;
    }

    @Bean
    public PlatformTransactionManager transactionManager() {
        JmsTransactionManager jmsTransactionManager = new JmsTransactionManager();
        jmsTransactionManager.setConnectionFactory(receiverActiveMQConnectionFactory());
        return jmsTransactionManager;
    }

    @Bean
    public Processor jmsToKafkaProcessor(){
         return new JmsToKafkaProcessor(sourceDestinationConfig ,applicationContext,schemaRegistryUrl);
    }

    @Bean
    public Processor jmsToMongoProcessor(){
         return new JmsToMongoProcessor(sourceDestinationConfig,schemaRegistryUrl);
    }

}
