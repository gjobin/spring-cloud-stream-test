package com.example.stream;

import com.example.model.Person;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Service
@Slf4j
public class PersonStream {

    @Bean
    public Supplier<Message<Person>> source() {
        return () -> MessageBuilder.withPayload(Person.builder()
                        .firstname("fn")
                        .lastname("ln")
                        .build())
                .setHeader(KafkaHeaders.KEY, UUID.randomUUID().toString())
                .build();
    }

    @Bean
    public Function<Person, Person> uppercase() {
        return p -> {
            p.setLastname(p.getLastname().toUpperCase());
            return p;
        };
    }

    @Bean
    public Consumer<Person> sink() {
        return p -> log.info(p.toString());
    }
}
