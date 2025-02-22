package com.kafka.reactive.kafka.sec06;

public class KafkaConsumerGroup {
/*
* Range assignor
*    0,1,2, 3, 4
*
* */
    private static class Consumer1{
        public static void main(String[] args) {
            KafkaConsumer.start("1");
        }
//        all
//        0,1,2
    }

    private static class Consumer2{
        public static void main(String[] args) {
            KafkaConsumer.start("2");
        }
//        3,4
    }

    private static class Consumer3{
        public static void main(String[] args) {
            KafkaConsumer.start("3");
        }
    }

    private static class Consumer4{
        public static void main(String[] args) {
            KafkaConsumer.start("4");
        }
    }
    private static class Consumer5{
        public static void main(String[] args) {
            KafkaConsumer.start("5");
        }
    }
}
