package com.microsoft.example;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.io.PrintWriter;
import java.io.File;
import java.lang.Exception;

// Handle starting producer or consumer
public class Run {
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        if(args.length < 3) {
            usage();
        }
        // Get the brokers
        String brokers = args[2] ; //"172.24.2.71:5702,172.24.3.49:5702";
        String topicName =  args[1]; // "testcsv2";//
        String csvLocation = args[3]; // "D:\\working\\hdinsight-kafka-java-get-started-master\\Producer-Consumer\\StreamFile_01.csv";
        String caseStr = args[0].toLowerCase(); // "consumer";
        switch(caseStr) {
            case "producer":
                Producer.produce(brokers, topicName,csvLocation);
                break;
            case "consumer":
                // Either a groupId was passed in, or we need a random one
                String groupId;              
                groupId = UUID.randomUUID().toString();
                Consumer.consume(brokers, groupId, topicName, csvLocation);
                break;
            case "describe":
                AdminClientWrapper.describeTopics(brokers, topicName);
                break;
            case "create":
                AdminClientWrapper.createTopics(brokers, topicName);
                break;
            case "delete":
                AdminClientWrapper.deleteTopics(brokers, topicName);
                break;
            default:
                usage();
        }
        System.exit(0);
    }
    // Display usage
    public static void usage() {
        System.out.println("Usage:");
        System.out.println("kafka-example.jar <producer|consumer|describe|create|delete> <topicName> brokerhosts [groupid]");
        System.exit(1);
    }
}
