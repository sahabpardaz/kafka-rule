package ir.sahab.kafkarule;

import ir.sahab.zookeeperrule.ZooKeeperRule;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class KafkaRuleTest {

    private static final String ZK_ADDRESS = "127.0.1.1:" + KafkaRule.anOpenPort();
    private static final String TOPIC_NAME = "test-topic";

    @ClassRule
    public static KafkaRule kafkaRule = new KafkaRule(ZK_ADDRESS);

    @ClassRule
    public static ZooKeeperRule zkRule = new ZooKeeperRule(ZK_ADDRESS);

    @ClassRule
    public static KafkaRule kafkaRuleWithSelfManagedZk = new KafkaRule();

    private static Properties properties = new Properties();
    static {
        properties.setProperty("message.max.bytes","5000000");
    }
    @ClassRule
    public static KafkaRule kafkaRuleWithSpecificProperties = new KafkaRule(properties);

    @BeforeClass
    public static void before() {
        kafkaRuleWithSelfManagedZk.createTopic(TOPIC_NAME, 1);
        kafkaRule.createTopic(TOPIC_NAME, 1);
    }

    @Test
    public void testKafkaRuleWithSelfManagedZkServer() {
        checkTopicIsClear();

        KafkaProducer<byte[], byte[]> kafkaProducer = kafkaRuleWithSelfManagedZk.newProducer();
        kafkaProducer.send(new ProducerRecord<>(TOPIC_NAME, "key".getBytes(), "value".getBytes()));
        kafkaProducer.close();

        KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaRuleWithSelfManagedZk.newConsumer();
        kafkaConsumer.subscribe(Collections.singleton(TOPIC_NAME));
        ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(1000);
        for (ConsumerRecord<byte[], byte[]> record : records) {
            Assert.assertArrayEquals("key".getBytes(), record.key());
            Assert.assertArrayEquals("value".getBytes(), record.value());
        }
        kafkaConsumer.close();

        makeTopicDirty();
    }

    @Test
    public void testDefaultProducerAndConsumer() {
        checkTopicIsClear();

        final String prefixKey = "test-key-";
        final String prefixValue = "test-value-";
        final int numRecords = 1000;

        KafkaProducer<byte[], byte[]> kafkaProducer = kafkaRule.newProducer();
        for (int i = 0; i < numRecords; i++) {
            ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>(TOPIC_NAME, (prefixKey + i).getBytes(),
                            (prefixValue + i).getBytes());
            kafkaProducer.send(record);
        }
        kafkaProducer.close();

        KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaRule.newConsumer();
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC_NAME));

        int count = 0;
        while (count < numRecords) {
            ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(1000);
            for (ConsumerRecord<byte[], byte[]> record : records) {
                Assert.assertArrayEquals((prefixKey + count).getBytes(), record.key());
                Assert.assertArrayEquals((prefixValue + count).getBytes(), record.value());
                count++;
            }
        }
        kafkaConsumer.close();

        makeTopicDirty();
    }

    @Test
    public void testCustomProducerAndConsumer() {
        checkTopicIsClear();

        final String prefixKey = "test-key-";
        final String prefixValue = "test-value-";
        final int numRecords = 1000;

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> kafkaProducer = kafkaRule.newProducer(producerProps);
        for (int i = 0; i < numRecords; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC_NAME, prefixKey + i, prefixValue + i);
            kafkaProducer.send(record);
        }
        kafkaProducer.close();

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        KafkaConsumer<String, String> kafkaConsumer = kafkaRule.newConsumer(consumerProps);
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC_NAME));

        int count = 0;
        while (count < numRecords) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                Assert.assertEquals(prefixKey + count, record.key());
                Assert.assertEquals(prefixValue + count, record.value());
                count++;
            }
        }
        kafkaConsumer.close();

        makeTopicDirty();
    }

    private void makeTopicDirty() {
        KafkaProducer<byte[], byte[]> kafkaProducer = kafkaRule.newProducer();
        kafkaProducer.send(new ProducerRecord<>(TOPIC_NAME, "key".getBytes(), "value".getBytes()));
        kafkaProducer.close();
    }

    private void checkTopicIsClear() {
        KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaRule.newConsumer();
        kafkaConsumer.subscribe(Collections.singleton(TOPIC_NAME));
        ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(1000);
        Assert.assertTrue(records.isEmpty());
        kafkaConsumer.close();
    }
}