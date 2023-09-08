package com.example.stream;

import com.example.Application;
import com.example.model.Person;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootTest(classes = Application.class)
@EmbeddedKafka(topics = {"source-out-0", "sink-in-0"})
class PersonStreamIT {
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    public void e2e() {
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("test", "false", embeddedKafkaBroker));
        Consumer<String, Person> consumer = new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), new JsonDeserializer<Person>()).createConsumer();
        consumer.subscribe(List.of("sink-in-0"));
        consumer.poll(Duration.ZERO);

        ConsumerRecord<String, Person> record = KafkaTestUtils.getSingleRecord(consumer, "sink-in-0", Duration.ofSeconds(5));
        Assertions.assertThat(record.value()).isEqualTo(Person.builder().firstname("fn").lastname("LN").build());

        consumer.close();
    }
}

/*
Payload and Headers when running the app vs running the test

Running App
-----------
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


Running Test
------------
Person(firstname=fn, lastname=LN)
source-type : kafka
id : 420355ec-2875-92da-cd83-d0d577142ec3
contentType : application/json
timestamp : 1694122858406
 */