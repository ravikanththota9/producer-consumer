package com.microsoft.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.Properties;
import java.util.Random;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Producer
{
    public static void produce(String brokers, String topicName, String csvLocation) throws IOException, InterruptedException
    {

        // Set properties used to configure the producer
        Properties properties = new Properties();
        // Set the brokers (bootstrap servers)
        properties.setProperty("bootstrap.servers", brokers);
        // Set how to serialize key/value pairs
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        // specify the protocol for Domain Joined clusters
        //properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        
        /**
         * Reading csv file and pushing data to kafka topic .
         */
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        try {
        	
            br = new BufferedReader(new FileReader(csvLocation));
            System.out.println("sending  records to kafka topic .....");
            while ((line = br.readLine()) != null) {
            	
                // use comma as separator
                String[] data = line.split(cvsSplitBy);
                //System.out.println(data[0] +" , "+data[1] + " , " + data[2] +" , "+data[3] + " , " + data[4] +" , "+data[5]);
                StringBuffer sb = new StringBuffer();
                sb.append(data[0]);
                sb.append(",");
                sb.append(data[1]);
                sb.append(",");
                sb.append(data[2]);
                sb.append(",");
                sb.append(data[3]);
                sb.append(",");
                sb.append(data[4]);
                sb.append(",");
                sb.append(data[5]);
                try
                {
                    producer.send(new ProducerRecord<String, String>(topicName, sb.toString())).get();
                }
                catch (Exception ex)
                {
                	ex.printStackTrace();
                    System.out.print(ex.getMessage());
                    throw new IOException(ex.toString());
                }

            }
            System.out.println("Records sent to kafka topic ");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }     
        

       /* // So we can generate random sentences
        Random random = new Random();
        String[] sentences = new String[] {
                "the cow jumped over the moon",
                "an apple a day keeps the doctor away",
                "four score and seven years ago",
                "snow white and the seven dwarfs",
                "i am at two with nature"
        };

        String progressAnimation = "|/-\\";
        // Produce a bunch of records
        for(int i = 0; i < 100; i++) {
            // Pick a sentence at random
            String sentence = sentences[random.nextInt(sentences.length)];
            // Send the sentence to the test topic
            try
            {
                producer.send(new ProducerRecord<String, String>(topicName, sentence)).get();
            }
            catch (Exception ex)
            {
            	ex.printStackTrace();
                System.out.print(ex.getMessage());
                throw new IOException(ex.toString());
            }
            String progressBar = "\r" + progressAnimation.charAt(i % progressAnimation.length()) + " " + i;
            System.out.write(progressBar.getBytes());
        }*/
    }
}
