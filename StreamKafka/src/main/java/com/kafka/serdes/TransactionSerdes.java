package com.kafka.serdes;

import com.kafka.event.Transaction;
import org.apache.kafka.common.serialization.Serdes;

public class TransactionSerdes extends Serdes.WrapperSerde<Transaction> {

    public TransactionSerdes() {
        super(new TransactionSerializer(), new TransactionDeserializer());
    }


}


