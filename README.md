# Rapid window processing of rare messages on Apache Flink

Watermark of Apache Flink is updated when new message is processed. If the message is rarely arrived, the watermark will be kept in same timestamp for a long time.
Flink time window (eg. Tumbling, Sliding, and Session window) is initiated only when the timestamp of the watermark is greater than window end timestamp, therefore, the window processing could extreamly be delayed for rarely arriving messages.

However, it is considerable for special case like below condition:
* WindowFunction is required
* Given bounded time, the system should respond to arrived messages
* Health checking is given for source systems

Indeed, predicting next event-time is prohibitted on event-time based system, because event-time means the system time could be different to the real time. 
Therefore, this example should not be recommended for general purpose.

## Solutions

1. Fake messages from message source (eg. Kafka)
    * Advantages
        * Flink job only considers removing fake messages
    * Disadvantages
        * Additional external system is required
        * Increases of network traffic and message storaging

2. Fake messages in Flink source function
    * Advantage
        * Problem could be solved without help of external systems
    * Disadvantage
        * Flink job itself controls processing on restarting

In this example code, try second solution.

---

Sung Gon Yi  
<skonmeme@gmail.com>

