# Kafka Rule
JUnit rule which provides an embedded Kafka server. It can both setup its own ZK server or get the address of an available ZK server. The rule has also helper methods to create a Kafka producer or consumer which is ready to work with the Kafka server exposed by the rule.

## Sample Usage

```
private static final String TOPIC_NAME = "test-topic";

@ClassRule
public static KafkaRule kafkaRule = new KafkaRule();

@BeforeClass
public static void beforeClass() {
     kafkaRule.createTopic(TOPIC_NAME, 1 /*num partitions*/);
}

@Test
public void test() {
    String key = "key";
    String value = "value";
    
    try (KafkaProducer<String, String> kafkaProducer = kafkaRule.newProducer()) {
        kafkaProducer.send(new ProducerRecord(TOPIC_NAME, key, value));
    }

    try (KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaRule.newConsumer()) {
        kafkaConsumer.subscribe(Collections.singleton(TOPIC_NAME));
        ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(1000);
        Assert.assertEquals(1, records.size())
        Assert.assertArrayEquals(key.getBytes(), records.get(0).key);
        Assert.assertArrayEquals(value.getBytes(), records.get(0).value());
    }
}
``` 
It is also possible to use a shared available ZK server by the Kafka rule:
```
private static final String ZK_ADDRESS = "127.0.1.1:" + anOpenPort();

@ClassRule
public static KafkaRule kafkaRule = new KafkaRule(ZK_ADDRESS);

@ClassRule
public static ZooKeeperRule zkRule = new ZooKeeperRule(ZK_ADDRESS);
```

## Add it to your project
You can reference to this library by either of java build systems (Maven, Gradle, SBT or Leiningen) using snippets from this jitpack link:
[![](https://jitpack.io/v/sahabpardaz/kafka-rule.svg)](https://jitpack.io/#sahabpardaz/kafka-rule)
