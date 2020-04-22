package reactor.kafka.receiver.internals;

enum AckMode {
    AUTO_ACK,
    MANUAL_ACK,
    ATMOST_ONCE,
    EXACTLY_ONCE,
}
