package com.kafka.stream;


import com.kafka.event.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Configuration
@EnableKafkaStreams
public class FraudDetection {

    @Bean
    public KStream<String, String> fraudDetectionSystem(StreamsBuilder builder){

        KStream<String, String> kStream =  builder.stream("transaction");

        KStream<String, String> filterData =  kStream.filter((k,v)->alert(v))
                .peek((k,v)->log.warn("Check the payment  {} ", v));


        filterData.to("stream-topic");

        return filterData;
    }


    private Boolean alert(String value){
       try {
           Transaction transaction = new ObjectMapper().readValue(value, Transaction.class);
           return transaction.amount()>10000;
       } catch (IllegalArgumentException e) {
           throw new RuntimeException(e);
       }

    }

}

