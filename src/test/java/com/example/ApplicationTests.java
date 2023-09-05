package com.example;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.context.EmbeddedKafka;


@SpringBootTest(classes = Application.class)
@Import({TestChannelBinderConfiguration.class})
@EmbeddedKafka
class ApplicationTests {

    @Test
    void contextLoads() {
    }
}
