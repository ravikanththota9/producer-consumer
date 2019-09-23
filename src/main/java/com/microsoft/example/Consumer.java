package com.microsoft.example;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.IfExists;
import com.opencsv.CSVWriter;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Consumer {
    public static int consume(String brokers, String groupId, String topicName, String csvFileName) throws InterruptedException, ExecutionException, IOException {
        // Create a consumer
        KafkaConsumer<String, String> consumer;
        // Configure the consumer
        Properties properties = new Properties();
        // Point it to the brokers
        properties.setProperty("bootstrap.servers", brokers);
        // Set the consumer group (all consumers must belong to a group).
        properties.setProperty("group.id", groupId);
        // Set how to serialize key/value pairs
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        // When a group is first created, it has no offset stored to start reading from. This tells it to start
        // with the earliest record in the stream.
        properties.setProperty("auto.offset.reset","earliest");

        // specify the protocol for Domain Joined clusters
        //properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        /**
         * connecting to datalake
         */
        ADLStoreClient client;
        String APP_ID = "fda6b9f3-aa4f-432f-a6c4-62f8f12554a0";
        String APP_SECRET = "62RdZ*=a8T1/K_4snJNc=2?uzk:2rCPi";
        String dirName = "/reporting";
        String StoreAcct = "ukbipowerbi";

        String authority = "https://login.microsoftonline.com/f83a8c3d-6a2f-4721-a996-fb1fa7255dbe";
        String resourcUrl = "https://management.core.windows.net/";
        ExecutorService service = Executors.newFixedThreadPool(1);

        AuthenticationContext context = new AuthenticationContext(authority, true, service);

        // Acquire Token
        Future<AuthenticationResult> result = context.acquireToken(
                resourcUrl,
                new ClientCredential(APP_ID, APP_SECRET),
                null
        );
        String token = result.get().getAccessToken();
        System.out.println("token " + token);

        String account = StoreAcct + ".azuredatalakestore.net";
        client = ADLStoreClient.createClient(account, token);

        //client.createDirectory(dirName);
        System.out.println("datalake connection created....");
        
        boolean fileExits = false;
        List<DirectoryEntry> list = client.enumerateDirectory("/reporting", 2000);
    	for (DirectoryEntry entry : list) {
    	    
    	    if(csvFileName.equalsIgnoreCase(entry.name)) {
    	    	System.out.println("File already exists");
    	    	fileExits = true;
    	    }
    	}
    	if(!fileExits) {
    		String filename = "/reporting/"+csvFileName;
    		OutputStream stream = client.createFile(filename, IfExists.OVERWRITE  );
    		stream.close();
    		System.out.println(csvFileName + " File created.");
    	}

        consumer = new KafkaConsumer<>(properties);

        // Subscribe to the 'test' topic
        consumer.subscribe(Arrays.asList(topicName));

        // Loop until ctrl + c
        List<String> dataList = new ArrayList<>();
        int count = 0;
        System.out.println("Kafka consumer started");
        while(true) {
            // Poll for records
            ConsumerRecords<String, String> records = consumer.poll(2);
            // Did we get any?
            if (records.count() == 0) {
                // timeout/nothing to read
            } else {
                // Yes, loop over records
                for(ConsumerRecord<String, String> record: records) {
                    // Display record and count
                    count += 1;
                    dataList.add(record.value());
                    if(dataList.size() == 100) {
                    	
                    	boolean fileExits1 = false;
                        List<DirectoryEntry> list1 = client.enumerateDirectory("/reporting", 2000);
                    	for (DirectoryEntry entry : list1) {
                    	    if(csvFileName.equalsIgnoreCase(entry.name)) {
                    	    	fileExits1 = true;
                    	    }
                    	}
                    	if(!fileExits1) {
                    		String filename = "/reporting/"+csvFileName;
                    		OutputStream stream = client.createFile(filename, IfExists.OVERWRITE  );
                    		stream.close();
                    		System.out.println(csvFileName + " File created.");
                    	}
                    	
                    	System.out.println("Writing data to file");
                    	String filename = "/reporting/"+csvFileName;
                    	OutputStream stream = client.getAppendStream(filename);
                    	stream.write(getSampleContent(dataList));
                    	stream.close();
                    	//System.out.println("File appended.");
                    	
                    	
                    	/*File file = new File(csvLocation); 
                        try { 
                            // create FileWriter object with file as parameter 
                            FileWriter outputfile = new FileWriter(file); 
                      
                            // create CSVWriter object filewriter object as parameter 
                            CSVWriter writer = new CSVWriter(outputfile); 
                      
                            // adding header to csv 
                            String[] header = { "Name", "Class", "Marks" }; 
                            writer.writeNext(header); 
                      
                            // add data to csv 
                            for(String data : dataList) {
                            	String[] data1 = data.split("~");
                            	 writer.writeNext(data1); 
                            }                      
                            // closing writer connection 
                            writer.close(); 
                        } 
                        catch (IOException e) { 
                            // TODO Auto-generated catch block 
                            e.printStackTrace(); 
                        } */
                        
                        dataList = new ArrayList<>();
                    	
                    }
                    
                }
            }
        }
    }
    
    private static byte[] getSampleContent(List<String> dataList) {
        ByteArrayOutputStream s = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(s);
        for(String data : dataList) {
        	out.println(data);
        }
        out.close();
        return s.toByteArray();
    }
}
