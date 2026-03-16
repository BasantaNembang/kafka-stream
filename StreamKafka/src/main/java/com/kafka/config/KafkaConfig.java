package com.kafka.config;


import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {


    @Bean
    public NewTopic createTransactionTopic(){
        return new NewTopic("transaction", 2, (short) 1);
    }

    @Bean
    public NewTopic createStreamTopic(){
        return new NewTopic("stream-topic", 2, (short) 1);
    }


}


