package com.kafka.service;


import com.kafka.event.Transaction;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.List;

@Service
public class KafkaStreamService {

    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

    public KafkaStreamService(KafkaTemplate<String, Transaction> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String performTransaction() {
//        for(int i=0; i<50; i++){
//            String transactionId = System.currentTimeMillis() + "-" + i;
//            double amount = 8000 + new Random().nextDouble() * (11000 - 8000);
//
//            Transaction txn = new Transaction(
//                    transactionId,
//                    "USER_" + i,
//                    amount, LocalDateTime.now().toString());
//
//            kafkaTemplate.send("transaction", transactionId, txn);
//        }


        List<Transaction> transactions =  getTransactionDataFromResources();
        for(Transaction tx : transactions){
            kafkaTemplate.send("transaction", tx.transactionId(), tx);
        }
        return "success";
    }


    private List<Transaction> getTransactionDataFromResources(){
       try(InputStream inputStream = getClass().getResourceAsStream("/transactions.json")){
           return mapper.readValue(inputStream, new TypeReference<List<Transaction>>() {
           });
       } catch (Exception e) {
           throw new RuntimeException(e);
       }
    }


}


