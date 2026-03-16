package com.kafka.serdes;


import com.kafka.event.Transaction;
import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.databind.ObjectMapper;

public class TransactionDeserializer implements Deserializer<Transaction> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Transaction deserialize(String s, byte[] bytes) {
        try {
        return mapper.readValue(bytes, Transaction.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}



