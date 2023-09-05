package com.example.stream;

import com.example.model.Person;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.valfirst.slf4jtest.TestLogger;
import com.github.valfirst.slf4jtest.TestLoggerFactory;
import com.github.valfirst.slf4jtest.TestLoggerFactoryExtension;
import com.example.Application;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.github.valfirst.slf4jtest.LoggingEvent.info;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIterable;

@SpringBootTest(classes = Application.class)
@Import({TestChannelBinderConfiguration.class})
@ExtendWith(TestLoggerFactoryExtension.class)
@EmbeddedKafka
class PersonStreamTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private InputDestination input;

    @Autowired
    private OutputDestination output;


    @Test
    void producerTest() {
        Message<Person> message = receiveMessage(Person.class, "thasource.destination");
        assertThat(message.getPayload()).isEqualTo(Person.builder().firstname("fn").lastname("ln").build());
        String key = (String) message.getHeaders().get("kafka_messageKey");
        assert key != null;
        assertThat(UUID.fromString(key).toString()).isEqualTo(key);
    }

    @Test
    void functionTest() {
        MessageHeaders messageHeaders = new MessageHeaders(Map.of("kafka_messageKey", "theKey"));
        Message<Person> sendMessage = new GenericMessage<>(Person.builder().firstname("firstname").lastname("lastname").build(), messageHeaders);
        output.clear();
        input.send(sendMessage, "thasource");

        Message<Person> message = receiveMessage(Person.class, "uppercased");
        assertThat(message.getPayload()).isEqualTo(Person.builder().firstname("firstname").lastname("LASTNAME").build());
        String key = (String) message.getHeaders().get("kafka_messageKey");
        assert key != null;
        assertThat(key).isEqualTo("theKey");
    }

    @Test
    void consumerTest() {
        MessageHeaders messageHeaders = new MessageHeaders(Map.of("kafka_messageKey", "theKey"));
        Person entity = Person.builder().firstname("firstname").lastname("LASTNAME").build();
        Message<Person> sendMessage = new GenericMessage<>(entity, messageHeaders);
        output.clear();

        TestLogger logger = TestLoggerFactory.getTestLogger(PersonStream.class);
        input.send(sendMessage, "uppercased");
        assertThatIterable(logger.getLoggingEvents()).isEqualTo(List.of(info(entity.toString())));
    }

    private <T> Message<T> receiveMessage(Class<T> clazz, String bindingName) {
        Message<byte[]> receivedMessage = output.receive(1000, bindingName);
        assertThat(receivedMessage).isNotNull();
        return new GenericMessage<>(deserializePayload(clazz, receivedMessage), receivedMessage.getHeaders());
    }

    private <T> T deserializePayload(Class<T> clazz, Message<byte[]> m) {
        try {
            return objectMapper.readValue(m.getPayload(), clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}