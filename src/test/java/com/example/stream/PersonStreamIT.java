package com.example.stream;

import com.example.Application;
import com.example.model.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * IT tests using EmbeddedKafka
 */
@SpringBootTest(classes = Application.class)
@ActiveProfiles(profiles = {"default", "test"})
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
        try (JsonDeserializer<Person> jsonDeserializer = new JsonDeserializer<Person>().trustedPackages("*");
             Consumer<String, Person> consumer = new DefaultKafkaConsumerFactory<>(consumerConfigs, new StringDeserializer(), jsonDeserializer).createConsumer()) {
            consumer.subscribe(List.of("source-out-0"));
            ConsumerRecord<String, Person> record = KafkaTestUtils.getSingleRecord(consumer, "source-out-0", Duration.ofSeconds(2));
            Assertions.assertThat(record.value()).isEqualTo(Person.builder().firstname("firstname1").lastname("lastname1").build());
        }
    }

    @Test
    public void testAppProducer() {
        //Assert message
        Map<String, Object> consumerConfigs = new HashMap<>(KafkaTestUtils.consumerProps("test", "false", embeddedKafkaBroker));
        try (JsonDeserializer<Person> jsonDeserializer = new JsonDeserializer<Person>().trustedPackages("*");
             Consumer<String, Person> consumer = new DefaultKafkaConsumerFactory<>(consumerConfigs, new StringDeserializer(), jsonDeserializer).createConsumer()) {
            consumer.subscribe(List.of("source-out-0"));
            ConsumerRecord<String, Person> record = KafkaTestUtils.getSingleRecord(consumer, "source-out-0", Duration.ofSeconds(2));
            Assertions.assertThat(record.value()).isEqualTo(Person.builder().firstname("ln").lastname("fn").build());
        }
    }
}

/*
Payload and Headers when running the app vs running the test

Running App actual application
-------------------------------

------ Consumed Record ------
Person(firstname=fn, lastname=LN)
deliveryAttempt : 1
kafka_timestampType : CREATE_TIME
kafka_receivedTopic : sink-in-0
kafka_offset : 46
scst_nativeHeadersPresent : true
kafka_consumer : org.apache.kafka.clients.consumer.KafkaConsumer@2994db0
source-type : kafka
id : 97a186be-5fcb-cc47-26a1-46b9895f6b32
kafka_receivedPartitionId : 0
contentType : application/json
kafka_receivedTimestamp : 1694174130515
kafka_groupId : anonymous.754c77d9-b7a7-4221-9132-34b5c347e299
timestamp : 1694174130552


Running Test, seems like application is not really connected to Kafka.
----------------------------------------------------------------------

------ Consumed Record ------
Person(firstname=fn, lastname=LN)
source-type : kafka
id : 420355ec-2875-92da-cd83-d0d577142ec3
contentType : application/json
timestamp : 1694122858406
 */