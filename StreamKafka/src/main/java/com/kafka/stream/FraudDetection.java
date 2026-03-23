package com.kafka.stream;

import com.kafka.event.Item;
import com.kafka.event.Transaction;
import com.kafka.serdes.TransactionSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Slf4j
@Configuration
@EnableKafkaStreams
public class FraudDetection {

    @Bean
    public KStream<String, Transaction> fraudDetectionSystem(StreamsBuilder builder){

        KStream<String, Transaction> kStream =
                builder.stream("transaction", Consumed.with(Serdes.String(), new TransactionSerdes()));

//         kStream.filter((k, v)->v.amount()>10000)
//                 .peek((k,v)->log.warn("check the payment {} ", v))
//                 .to("stream-topic");


//        kStream.map((key,value)->{
//            return KeyValue.pair(value.transactionId(), "Amount send is  " + value.amount());
//        }).peek((k,v)->{
//            log.info("The key and  value is  {} {} ", k, v);
//        });


//        kStream.mapValues(Transaction::amount)
//                .peek((k,v)->{
//                    log.info("The key and  value is  {} {} ", k, v);
//                });



//        kStream.flatMap((k,v)->{
//            List<KeyValue<String,Item>> values = new ArrayList<>();
//            for(Item item : v.items()){
//                values.add(new KeyValue<>(v.transactionId(), item));
//            }
//            return values;
//        }).peek((k,v)->{
//            log.info("The Id and item is {} {} ", k, v);
//        });


//        kStream.flatMapValues(Transaction::items)
//                .peek((k,v)->{
//                    log.info("The Id and item is {} {} ", k, v);
//                });


        //GROUP IN DIFFERENT-DIFFERENT GROUP
//         kStream.split()
//                 .branch((k,v)->
//                     v.type().equalsIgnoreCase("credit"),
//                         Branched.withConsumer((k)->{
//                             k.peek((k1,v)->log.info("Transaction in CREDIT {} " , v));
//                             k.to("credit-topic");
//                             })
//                 )
//                 .branch((k,v)->
//                         v.type().equalsIgnoreCase("debit"),
//                         Branched.withConsumer((k)->{
//                             k.peek((k1,v)->log.info("Transaction in DEBIT {} " , v));
//                             k.to("debit-topic");
//                         }));


//        kStream
//                .groupBy((k,v)->v.location())
//                .count()
//                .toStream()
//                .peek((k,v)->
//                   log.info("The number of order {} form is {} " , k , v)
//                );


        //CALCULATE THE SUM
        kStream
                .groupBy((k,v)->v.type())
                .aggregate(
                        ()->0.0,
                        (key, txt, sum)->sum+ txt.amount(),
                        Materialized.with(Serdes.String(), Serdes.Double())
                ).toStream()
                .peek((k,v)->
                   log.info("The used card {} and the total amount is  {} " , k , v)
                );

        return kStream;
    }



}

