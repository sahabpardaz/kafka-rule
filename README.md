# kafka-rule
JUnit rule which provides an embedded Kafka server. The rule has also helper methods to get producers and consumers by default or custom configuration to work with that Kafka server.
The rule can initialize in two ways, first setup an embedded Kafka server in the case that it should setup its own embedded ZooKeeper server and second way setup embedded Kafka server that work with external ZooKeeper server.

## Sample Usage

### Define topic name and Kafka rule

```
private static final String TOPIC_NAME = "test-topic";

@ClassRule
public static KafkaRule kafkaRule = new KafkaRule();
```
### Create topic

```
kafkaRule.createTopic(TOPIC_NAME, /*numPartitions*/ 1);
```
 
### Produce and consume

```
@Test
public void test(){
    String key = "key";
    String value = "value";
    KafkaProducer<byte[], byte[]> kafkaProducer = kafkaRule.newProducer();
    kafkaProducer.send(new ProducerRecord(TOPIC_NAME, key.getBytes(), value.getBytes()));
    kafkaProducer.close();

    KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaRule.newConsumer();
    kafkaConsumer.subscribe(Collections.singleton(TOPIC_NAME));
    ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(1000);
    for (ConsumerRecord<byte[], byte[]> record : records) {
        Assert.assertArrayEquals(key.getBytes(), record.key());
        Assert.assertArrayEquals(value.getBytes(), record.value());
    }
    kafkaConsumer.close();
}
``` 
## Add it to your project
You can reference to this library by either of java build systems (Maven, Gradle, SBT or Leiningen) using snippets from this jitpack link:
[![](https://jitpack.io/v/sahabpardaz/kafka-rule.svg)](https://jitpack.io/#sahabpardaz/kafka-rule)
