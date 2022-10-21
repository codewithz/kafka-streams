package com.codewithz.kafka.streams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FraudDetectionApplication {

    private static Logger LOG = LoggerFactory.getLogger(FraudDetectionApplication.class);

    public static void main(String[] args) {

        // Topics:
        //     "payment" -> "validated-payments"

        // Message key:
        //     String transactionId
        // Message value ( order ):
        //     String userId
        //     Integer nbOfItems
        //     Float totalAmount

    }

}
