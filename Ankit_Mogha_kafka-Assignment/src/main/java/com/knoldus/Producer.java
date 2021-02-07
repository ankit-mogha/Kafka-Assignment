package com.knoldus;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class Producer {
    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("max.block.ms",1000);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.knoldus.UserSerializer");
        
        KafkaProducer kafkaProducer = new KafkaProducer<String, User>(properties);
        Random rand = new Random();
        try{
            for(int i=1; i<=10; i++) {
                User user = new User(i, "Ankit Mogha", rand.nextInt((30 - 10) + 1) + 10, "MCA");
                kafkaProducer.send(new ProducerRecord<String, User>("user", user));
                System.out.println("Message Sent : " + user.toString());
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }
}