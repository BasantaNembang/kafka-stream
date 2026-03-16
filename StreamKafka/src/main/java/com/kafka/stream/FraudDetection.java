package com.kafka.stream;


import com.kafka.event.Transaction;
import com.kafka.serdes.TransactionSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Slf4j
@Configuration
@EnableKafkaStreams
public class FraudDetection {

    @Bean
    public KStream<String, Transaction> fraudDetectionSystem(StreamsBuilder builder){

        KStream<String, Transaction> kStream =
                builder.stream("transaction", Consumed.with(Serdes.String(), new TransactionSerdes()));

         kStream.filter((k, v)->v.amount()>10000)
                 .peek((k,v)->log.warn("check the payment {} ", v))
                 .to("stream-topic");

        return kStream;
    }



}

