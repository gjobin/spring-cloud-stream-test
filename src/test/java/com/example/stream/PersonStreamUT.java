package com.example.stream;

import com.example.Application;
import com.example.model.Person;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests using the TestChannelBinderConfiguration
 */
@SpringBootTest(classes = Application.class)
@Import({TestChannelBinderConfiguration.class})
public class PersonStreamUT {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private InputDestination input;

    @Autowired
    private OutputDestination output;

    @BeforeEach
    public void beforeEach() {
        output.clear();
    }

    @Test
    void producerTest() {
        Message<Person> message = receiveMessage(Person.class, "source-out-0");
        assertThat(message.getPayload()).isEqualTo(Person.builder().firstname("fn").lastname("ln").build());
    }

    @Test
    void functionTestBinder() {
        Message<Person> sendMessage = new GenericMessage<>(Person.builder().firstname("firstname").lastname("lastname").build(), new MessageHeaders(Collections.emptyMap()));
        input.send(sendMessage, "source-out-0");

        Message<Person> message = receiveMessage(Person.class, "sink-in-0");
        assertThat(message.getPayload()).isEqualTo(Person.builder().firstname("firstname").lastname("LASTNAME").build());
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
