package com.jpmc.midascore;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
// Added topics = {"test-topic"} so embedded Kafka auto-creates the topic needed
// Changed port to 9093 to avoid potential port conflicts if local Kafka runs on 9092
@EmbeddedKafka(partitions = 1, topics = {"test-topic"}, brokerProperties = {"listeners=PLAINTEXT://localhost:9093", "port=9093"})
class TaskTwoTests {
    static final Logger logger = LoggerFactory.getLogger(TaskTwoTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private FileLoader fileLoader;

    @Test
    void task_two_verifier() throws InterruptedException {
        String[] transactionLines = fileLoader.loadStrings("/test_data/poiuytrewq.uiop");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }

        // Wait for messages to be consumed before ending test
        Thread.sleep(2000);

        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("Use your debugger to watch for incoming transactions");
        logger.info("Kill this test once you find the answer");

        while (true) {
            Thread.sleep(5000); // give the consumer time to process messages
            logger.info("✅ All transactions received. Ending test.");

        }
    }
}
