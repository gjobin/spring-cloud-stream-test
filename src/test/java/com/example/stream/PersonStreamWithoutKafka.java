package com.example.stream;

import com.example.Application;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * IT tests without any Kafka
 */
@SpringBootTest(classes = Application.class)
@ActiveProfiles(profiles = {"default", "test"})
@Slf4j
class PersonStreamWithoutKafka {

    @Test
    public void testAppStartsWithoutKafka() throws InterruptedException {
        Thread.sleep(5000);
        //THIS SHOULD NOT PROCESS MESSAGE, KAFKA IS NOT RUNNING.
    }
}