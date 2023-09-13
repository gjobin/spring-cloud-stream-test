package com.example.stream;

import com.example.Application;
import com.example.model.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * IT tests using EmbeddedKafka
 */
@SpringBootTest(classes = Application.class)
@EmbeddedKafka
@Slf4j
class PersonStreamIT {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    public void testEmbeddedKafka() {
        //Send message
        Map<String, Object> producerConfigs = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        try (KafkaProducer<String, Person> producer = new KafkaProducer<>(producerConfigs, new StringSerializer(), new JsonSerializer<>())) {
            producer.send(new ProducerRecord<>("source-out-0", new Person("firstname1", "lastname1")));
        }

        //Assert message
        Map<String, Object> consumerConfigs = new HashMap<>(KafkaTestUtils.consumerProps("test", "false", embeddedKafkaBroker));
        try (JsonDeserializer<Person> jsonDeserializer = new JsonDeserializer<>(Person.class).trustedPackages("*");
             Consumer<String, Person> consumer = new DefaultKafkaConsumerFactory<>(consumerConfigs, new StringDeserializer(), jsonDeserializer).createConsumer()) {
            consumer.subscribe(List.of("source-out-0"));
            ConsumerRecords<String, Person> record = KafkaTestUtils.getRecords(consumer);
            Assertions.assertThat(record.records("source-out-0")).map(ConsumerRecord::value).contains(Person.builder().firstname("firstname1").lastname("lastname1").build());
        }
    }

    @Test
    public void testAppProducer() {
        //Assert message
        Map<String, Object> consumerConfigs = new HashMap<>(KafkaTestUtils.consumerProps("test", "false", embeddedKafkaBroker));
        try (JsonDeserializer<Person> jsonDeserializer = new JsonDeserializer<>(Person.class).trustedPackages("*");
             Consumer<String, Person> consumer = new DefaultKafkaConsumerFactory<>(consumerConfigs, new StringDeserializer(), jsonDeserializer).createConsumer()) {
            consumer.subscribe(List.of("source-out-0"));
            ConsumerRecords<String, Person> record = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(2));
            Assertions.assertThat(record.records("source-out-0")).map(ConsumerRecord::value).contains(Person.builder().firstname("fn").lastname("ln").build());
        }
    }

    @Test
    public void testAppFunction() {
        //Assert message
        Map<String, Object> consumerConfigs = new HashMap<>(KafkaTestUtils.consumerProps("test", "false", embeddedKafkaBroker));
        try (JsonDeserializer<Person> jsonDeserializer = new JsonDeserializer<>(Person.class).trustedPackages("*");
             Consumer<String, Person> consumer = new DefaultKafkaConsumerFactory<>(consumerConfigs, new StringDeserializer(), jsonDeserializer).createConsumer()) {
            consumer.subscribe(List.of("sink-in-0"));
            ConsumerRecords<String, Person> record = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(2));
            Assertions.assertThat(record.records("sink-in-0")).map(ConsumerRecord::value).contains(Person.builder().firstname("fn").lastname("LN").build());
        }
    }
}