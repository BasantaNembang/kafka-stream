package com.kafka.serdes;

import com.kafka.event.Transaction;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.ObjectMapper;

public class TransactionSerializer implements Serializer<Transaction> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(String s, Transaction transaction) {
        try{
         return mapper.writeValueAsBytes(transaction);
        } catch (RuntimeException e) {
            throw new RuntimeException(e);
        }
    }


}


