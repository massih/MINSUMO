package com.minsumo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;

/**
 * Created by massih on 4/11/16.
 */
public class DataHandler {

    private final String KAFKA_SERVER = "localhost:9092";
    private int sumoPort;
    private Producer<String, String> producer;
    private static final Logger LOG = LoggerFactory.getLogger(DataHandler.class);

    public DataHandler(int port) throws Exception {
        sumoPort = port;
        kafkaSetup();
        receiveVehicleData();
    }

    private void kafkaSetup(){
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_SERVER);
        props.put("acks", "0");
        props.put("retries", 0);
        props.put("batch.size", 0);
        props.put("linger.ms", 0);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    private void receiveVehicleData() throws Exception {
        ServerSocket serverSocket = new ServerSocket(sumoPort);
        System.out.println("Waiting :-D");
        Socket socket = serverSocket.accept();
        System.out.println("connected :-)");
        BufferedReader buffReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        int i = 0;
        boolean inElement = false;
        String element = "";
        String temp;
        while (i < 100){
            temp = buffReader.readLine().trim();
            if(inElement){
                element += temp;
                if (temp.startsWith("</timestep>")){
                    producer.send(new ProducerRecord<String, String>("unprocessed", element ) );
                    inElement = false;
                    i++;
                    //System.out.println(i + " - " + element);
                    element = "";
                }
            }else {
                if (temp.startsWith("<timestep")){
                    element += temp;
                    inElement = true;
                }
            }
        }
        producer.close();
    }
}