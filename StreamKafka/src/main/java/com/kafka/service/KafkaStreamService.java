package com.kafka.service;


import com.kafka.event.Transaction;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.Random;

@Service
public class KafkaStreamService {

    private final KafkaTemplate<String, Transaction> kafkaTemplate;


    public KafkaStreamService(KafkaTemplate<String, Transaction> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String performTransaction() {
        for(int i=0; i<50; i++){
            String transactionId = System.currentTimeMillis() + "-" + i;
            double amount = 8000 + new Random().nextDouble() * (11000 - 8000);

            Transaction txn = new Transaction(
                    transactionId,
                    "USER_" + i,
                    amount, LocalDateTime.now().toString());

            kafkaTemplate.send("transaction", transactionId, txn);
        }

        return "success";
    }


}


