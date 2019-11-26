package ir.sahab.kafkarule;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Properties;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode.Disabled$;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit rule which provides an embedded Kafka server.
 */
public class KafkaRule extends ExternalResource {

    private static final Logger logger = LoggerFactory.getLogger(KafkaRule.class);

    private String zkAddress;
    private String localIp;
    private int brokerPort;
    private KafkaServerStartable broker;
    private File logDir;

    private Properties additionalBrokerConfigs;

    private boolean selfManagedZooKeeper = false;
    private EmbeddedZkServer zkServer;

    /**
     * Creates a rule to setup an embedded Kafka server in the case that Kafka rule should setup its
     * own embedded ZooKeeper server.
     */
    public KafkaRule() {
        selfManagedZooKeeper = true;
    }

    /**
     * Creates a rule to setup an embedded Kafka server in the case that there is a ZooKeeper server
     * available and the Kafka broker is supposed to use that ZooKeeper.
     *
     * @param zkAddress the address of embedded ZooKeeper. It should be in format of "IP:PORT" and
     * the IP should be one of the IPs of the local system.
     */
    public KafkaRule(String zkAddress) {
        String[] splittedZkAddress = zkAddress.split(":");
        if (splittedZkAddress.length != 2) {
            throw new IllegalArgumentException(
                    "ZooKeeper address should be in the format of IP:PORT");
        }

        initAddresses(zkAddress);
    }

    /**
     * Creates a rule to setup an embedded Kafka server in the case that Kafka rule should setup its
     * own embedded ZooKeeper server.
     *
     * @param kafkaBrokerConfig additional kafka broker config.
     */
    public KafkaRule(Properties kafkaBrokerConfig){
        this();
        additionalBrokerConfigs = kafkaBrokerConfig;
    }

    /**
     * Creates a rule to setup an embedded Kafka server in the case that there is a ZooKeeper server
     * available and the Kafka broker is supposed to use that ZooKeeper.
     *
     * @param zkAddress the address of embedded ZooKeeper. It should be in format of "IP:PORT" and
     * the IP should be one of the IPs of the local system.
     * @param kafkaBrokerConfigs additional kafka broker configs.
     */
    public KafkaRule(String zkAddress, Properties kafkaBrokerConfigs){
        this(zkAddress);
        additionalBrokerConfigs = kafkaBrokerConfigs;
    }

    @Override
    protected void before() throws Throwable {
        if (selfManagedZooKeeper) {
            createAndStartEmbeddedZkServer();
        }

        logDir = Files.createTempDirectory("kafka").toFile();
        Properties kafkaBrokerConfig = new Properties();

        kafkaBrokerConfig.setProperty(KafkaConfig.ZkConnectProp(), zkAddress);
        kafkaBrokerConfig.setProperty(KafkaConfig.BrokerIdProp(), "1");
        // Configs 'host.name' and 'advertised.host.name' are deprecated since Kafka version 1.1.
        // Use 'listeners' and 'advertised.listeners' instead of them. See this:
        // https://kafka.apache.org/11/documentation.html#configuration
        kafkaBrokerConfig.setProperty(KafkaConfig.ListenersProp(),
                String.format("PLAINTEXT://%s:%s", localIp, brokerPort));
        kafkaBrokerConfig.setProperty(KafkaConfig.AdvertisedListenersProp(),
                String.format("PLAINTEXT://%s:%s", localIp, brokerPort));
        kafkaBrokerConfig.setProperty(KafkaConfig.PortProp(), Integer.toString(brokerPort));
        kafkaBrokerConfig.setProperty(KafkaConfig.LogDirProp(), logDir.getAbsolutePath());
        kafkaBrokerConfig.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), "1");
        kafkaBrokerConfig.setProperty(KafkaConfig.AutoCreateTopicsEnableProp(), "true");
        kafkaBrokerConfig.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
        if (additionalBrokerConfigs != null) {
            kafkaBrokerConfig.putAll(additionalBrokerConfigs);
        }
        broker = new KafkaServerStartable(new KafkaConfig(kafkaBrokerConfig));
        broker.startup();
    }

    @Override
    protected void after() {
        this.broker.shutdown();

        if (selfManagedZooKeeper) {
            try {
                zkServer.close();
            } catch (IOException e) {
                throw new AssertionError("Failed to stop embedded ZK server.", e);
            }
        }

        try {
            Files.walk(Paths.get(logDir.getPath()))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            throw new AssertionError("Failed to clear Kafka data directory.", e);
        }
    }

    private void createAndStartEmbeddedZkServer() {
        zkServer = new EmbeddedZkServer();
        try {
            zkServer.start();
        } catch (IOException | InterruptedException e) {
            throw new AssertionError("Failed to start embedded ZK server.", e);
        }

        initAddresses(zkServer.getAddress());
    }

    private void initAddresses(String zkAddress) {
        String[] splittedZkAddress = zkAddress.split(":");
        this.localIp = splittedZkAddress[0];
        this.zkAddress = zkAddress;
        this.brokerPort = anOpenPort();
    }

    public void createTopic(String topicName) {
        createTopic(topicName, 1);
    }

    public void createTopic(String topicName, int numPartitions) {
        ZkClient zkClient = null;
        ZkUtils zkUtils;
        try {
            // We have to pass the serializer class to ZkClient constructor Otherwise createTopic will return
            // without error. The topic will exist in zookeeper and be returned when listing topics, but Kafka
            // itself does not create the topic.
            zkClient = new ZkClient(zkAddress, 30000, 30000, ZKStringSerializer$.MODULE$);
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zkAddress), JaasUtils.isZkSecurityEnabled());
            logger.info("Executing create Topic: " + topicName + ", partitions: " + numPartitions
                    + ", replication-factor: 1.");
            AdminUtils.createTopic(zkUtils, topicName, numPartitions, 1, new Properties(), Disabled$.MODULE$);
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    /**
     * Creates a Kafka producer with mostly default configuration. It is responsibility of the
     * caller to close this producer.
     *
     * These are the non-default items which are set here:
     * <ul>
     * <li> BOOTSTRAP_SERVERS_CONFIG is set to local address of this Kafka broker </li>
     * <li> KEY_SERIALIZER_CLASS_CONFIG is set to ByteArraySerializer.class </li>
     * <li> KEY_SERIALIZER_CLASS_CONFIG is set to ByteArraySerializer.class </li>
     * </ul>
     */
    public KafkaProducer<byte[], byte[]> newProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerAddress());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        return new KafkaProducer<>(props);
    }

    /**
     * Creates Kafka producer with specific configuration. It adds broker address to the properties
     * if it is not provided. It is responsibility of the caller to close this producer.
     *
     * @param props the configuration of producer.
     */
    public <K, V> KafkaProducer<K, V> newProducer(Properties props) {
        addBrokerName(props);
        return new KafkaProducer<>(props);
    }

    /**
     * Creates s Kafka consumer with mostly default configuration. It is responsibility of the
     * caller to close this consumer.
     *
     * These are the non-default items which are set here:
     * <ul>
     * <li> BOOTSTRAP_SERVERS_CONFIG is set to local address of this Kafka broker </li>
     * <li> GROUP_ID_CONFIG is set to "test-group-id" </li>
     * <li> AUTO_OFFSET_RESET_CONFIG is set to "earliest" </li>
     * <li> KEY_DESERIALIZER_CLASS_CONFIG is set to ByteArrayDeserializer.class </li>
     * <li> VALUE_DESERIALIZER_CLASS_CONFIG is set to ByteArrayDeserializer.class </li>
     * </ul>
     */
    public KafkaConsumer<byte[], byte[]> newConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerAddress());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-id");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        return new KafkaConsumer<>(props);
    }

    /**
     * Creates Kafka consumer with specific configuration. It adds broker address to the properties
     * if it is not provided. It is responsibility of the caller to close this consumer.
     *
     * @param props the configuration of consumer.
     */
    public <K, V> KafkaConsumer<K, V> newConsumer(Properties props) {
        addBrokerName(props);
        return new KafkaConsumer<>(props);
    }

    /**
     * Adds broker address to the properties if it is not provided.
     */
    private void addBrokerName(Properties props) {
        String brokerAddress = props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        if (brokerAddress == null) {
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerAddress());
        } else if (!brokerAddress.equals(getBrokerAddress())) {
            throw new IllegalArgumentException(
                    "Broker address is different to this Kafka broker address");
        }
    }

    public String getZkAddress() {
        if (zkAddress == null) {
            throw new IllegalStateException("Zk server is not yet setup.");
        }
        return zkAddress;
    }

    public String getBrokerAddress() {
        if (localIp == null || brokerPort == 0) {
            throw new IllegalStateException("Kafka broker is not yet setup.");
        }
        return localIp + ":" + brokerPort;
    }

    static int anOpenPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new AssertionError("Unable to find an open port.", e);
        }
    }

    /**
     * Provide an embedded Zookeeper server.
     */
    private class EmbeddedZkServer implements Closeable {

        private String localIp;

        private ServerCnxnFactory factory;
        private File snapshotDir;
        private File logDir;
        private int port;

        /**
         * Starts an embedded ZooKeeper server. It should be called just once.
         */
        public void start() throws IOException, InterruptedException {
            snapshotDir = Files.createTempDirectory("zk-snapshot").toFile();
            logDir = Files.createTempDirectory("zk-logs").toFile();

            // Why we are going to use local IP and not just localhost or 127.0.0.1 constants?
            // Because we have encountered a problem when configured an KafkaServerStartable
            // to use this embedded ZooKeeper on 'localhost'.
            // But using local IP, solved the problem. See this:
            // https://www.ibm.com/support/knowledgecenter/SSPT3X_4.1.0/
            // com.ibm.swg.im.infosphere.biginsights.trb.doc/doc/trb_kafka_producer_localhost.html
            localIp = InetAddress.getLocalHost().getHostAddress();
            this.port = KafkaRule.anOpenPort();

            // ZooKeeperServer overrides DefaultUncaughtExceptionHandler
            // and we do not want anyone to override this behaviour.
            // So here, we are going to backup the DefaultUncaughtExceptionHandler before
            // creating ZkServer and restore it after.
            Thread.UncaughtExceptionHandler handler = Thread.getDefaultUncaughtExceptionHandler();

            ZooKeeperServer zkServer = new ZooKeeperServer(snapshotDir, logDir, 500 /*tick time*/);
            this.factory = NIOServerCnxnFactory.createFactory();
            this.factory.configure(new InetSocketAddress(localIp, port), 100 /*Max clients*/);
            this.factory.startup(zkServer);

            // Restore the DefaultUncaughtExceptionHandler.
            Thread.setDefaultUncaughtExceptionHandler(handler);
        }

        @Override
        public void close() throws IOException {
            if (factory != null) {
                factory.shutdown();
            }

            Files.walk(Paths.get(logDir.getPath()))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);

            Files.walk(Paths.get(snapshotDir.getPath()))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }

        public String getAddress() {
            return localIp + ":" + port;
        }

        public int getPort() {
            return port;
        }
    }

}