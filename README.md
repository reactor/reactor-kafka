Reactive Kafka API
===================

You need to have [Gradle 2.0 or higher](http://www.gradle.org/installation) and [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/index.html) installed.

### First bootstrap and download the wrapper ###
    cd reactor-kafka
    gradle

### Building a jar and running it ###
    ./gradlew jar

### Running unit tests ###
    ./gradlew test

### Building IDE project ###
    ./gradlew eclipse
    ./gradlew idea

### Sample producer and consumer ###

See [reactor-kafka-samples/src/main/java/reactor/kafka/samples/SampleProducer.java](reactor-kafka-samples/src/main/java/reactor/kafka/samples/SampleProducer.java) for sample reactive producer.
See [reactor-kafka-samples/src/main/java/reactor/kafka/samples/SampleConsumer.java](reactor-kafka-samples/src/main/java/reactor/kafka/samples/SampleConsumer.java) for sample reactive producer.

#### Setup Kafka cluster and create topic: ####
1. Start Zookeeper and Kafka server
2. Create topic "demo-topic"

#### To run sample producer: ####
1. Update BOOTSTRAP_SERVERS and TOPIC in SampleProducer.java if required
2. Compile and run reactor.kafka.samples.SampleProducer (eg. from IDE as a Java application))

#### To run sample consumer: ####
1. Update BOOTSTRAP_SERVERS and TOPIC in SampleConsumer.java if required
2.  Run reactor.kafka.samples.SampleConsumer (eg. from IDE as a Java application))


