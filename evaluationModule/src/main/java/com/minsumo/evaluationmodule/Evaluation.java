package com.minsumo.evaluationmodule;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by massih on 9/18/16.
 */
public class Evaluation {

    private final String KAFKA_SERVER = "localhost:9092";
    private KafkaConsumer<String, String> consumer;

    private long averageLatency;
    private long startTime;
    private long endTime;
    private int globalCounter;
    private int counter;
    private boolean latency = true;
    private final int AVERAGE_COUNTER = 1000;


    public Evaluation() {
        kafkaSetup();
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                calculateLatency(parseTuple(record.value()));
                //calculateThroughput(parseTuple(record.value()));
            //System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
            //calculateLatency(record.value());
        }
    }

    private void kafkaSetup(){
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_SERVER);
        props.put("group.id", "evaluation");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("toEvaluate"));
    }

    private Long[] parseTuple(String tuple){
        return new Long[]{Long.parseLong(tuple.substring(0, tuple.indexOf(";"))), Long.parseLong(tuple.substring(tuple.indexOf(";")+1))};
    }


    private void calculateLatency(Long[] TS){
        if(globalCounter >= 5000){
            if (counter <= AVERAGE_COUNTER){
                Long ltc = (TS[1] - TS[0]);
                averageLatency += ltc;

                if(counter == AVERAGE_COUNTER){
                    averageLatency = averageLatency/AVERAGE_COUNTER;
                    System.out.println("%%%%%%%%%%%%%%%%The average latency for 1000 tuple is: "+ averageLatency + " %%%%%%%%%%%%%%%%");
                    counter = 0;
                    averageLatency = 0;
                }
                counter++;
            }
        }else{
            globalCounter++;
        }
    }

    private void calculateThroughput(Long[] TS){
        if (globalCounter == 5000){
            startTime = System.currentTimeMillis()+10000;
            endTime = startTime + 10000;
            System.out.println(" START TIME = " + startTime);
            System.out.println(" END TIME = " + endTime);
            globalCounter++;
        }else if(globalCounter > 5000) {
            if(TS[0] >= startTime && TS[1] <= endTime){
                counter ++;
                System.out.println(counter/10 + " tuples processed in 1 second");
            }
        }else if(globalCounter < 5000){
            globalCounter++;
        }
    }

}
