package com.example.stream;

import com.example.model.Person;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Service
@Slf4j
public class PersonStream {

    @Bean
    public Supplier<Person> source() {
        return () -> Person.builder()
                .firstname("fn")
                .lastname("ln")
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
