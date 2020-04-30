Reactor Kafka
===================

[![Join the chat at https://gitter.im/reactor/reactor](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Travis CI](https://img.shields.io/travis/reactor/reactor-kafka.svg)](https://travis-ci.org/reactor/reactor-kafka)
[![Coverage](https://img.shields.io/codecov/c/github/reactor/reactor-kafka.svg)](https://travis-ci.org/reactor/reactor-kafka)

You need to have [Java 8](https://www.oracle.com/technetwork/java/javase/downloads/index.html) installed.

### Building Reactor Kafka jars ###
    ./gradlew jar

### Running unit tests ###
    ./gradlew test

### Building IDE project ###
    ./gradlew eclipse
    ./gradlew idea

### Sample producer and consumer ###

See [reactor-kafka-samples/src/main/java/reactor/kafka/samples/SampleProducer.java](reactor-kafka-samples/src/main/java/reactor/kafka/samples/SampleProducer.java) for sample reactive producer.
See [reactor-kafka-samples/src/main/java/reactor/kafka/samples/SampleConsumer.java](reactor-kafka-samples/src/main/java/reactor/kafka/samples/SampleConsumer.java) for sample reactive consumer.

#### Setup Kafka cluster and create topic: ####
1. Start Zookeeper and Kafka server
2. Create topic "demo-topic"

#### To run sample producer: ####
1. Update BOOTSTRAP_SERVERS and TOPIC in SampleProducer.java if required
2. Compile and run reactor.kafka.samples.SampleProducer (eg. from IDE as a Java application))

#### To run sample consumer: ####
1. Update BOOTSTRAP_SERVERS and TOPIC in SampleConsumer.java if required
2.  Run reactor.kafka.samples.SampleConsumer (eg. from IDE as a Java application))

#### To build applications using reactor-kafka API: ####

With Gradle from repo.spring.io:
```groovy
    repositories {
      //maven { url 'https://repo.spring.io/snapshot' }
      //maven { url 'https://repo.spring.io/milestone' }
      mavenCentral()
    }

    dependencies {
      //compile "io.projectreactor.kafka:reactor-kafka:1.3.1-SNAPSHOT"
      compile "io.projectreactor.kafka:reactor-kafka:1.3.0"
    }
```

### Community / Support ###

* [GitHub Issues](https://github.com/reactor/reactor-kafka/issues)

### License ###

Reactor Kafka is [Apache 2.0 licensed](https://www.apache.org/licenses/LICENSE-2.0.html).

