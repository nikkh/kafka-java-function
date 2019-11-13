package com.nick.kafka;

import java.util.*;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;     //v0.11.0.0
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;
import java.util.logging.Level;


/**
 * Azure Functions with HTTP Trigger.
 */
public class Send_kafka_batch {
    /**
     * This function listens at endpoint "/api/Send-kafka-batch". Two ways to invoke it using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your host}/api/Send-kafka-batch
     * 2. curl {your host}/api/Send-kafka-batch?name=HTTP%20Query
     */
    @FunctionName("Send-kafka-batch")
    public HttpResponseMessage run(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.FUNCTION) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        context.getLogger().info("Send-kafka_batch received a message.  Invocation Id is " + context.getInvocationId());
                
        String bootstrapServers = System.getenv("FLINK_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please ensure env: FLINK_BOOTSTRAP_SERVERS is set in application config").build();
        } 
        String clientId = System.getenv("FLINK_CLIENT_ID");
        if (clientId == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please ensure env: FLINK_CLIENT_ID is set in application config").build();
        } 
        
        String saslMechanism = System.getenv("FLINK_SASL_MECHANISM");
        if (saslMechanism == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please ensure env: FLINK_SASL_MECHANISM is set in application config").build();
        } 

        String securityProtocol = System.getenv("FLINK_SECURITY_PROTOCOL");
        if (securityProtocol == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please ensure env: FLINK_SECURITY_PROTOCOL is set in application config").build();
        } 
        
        String saslJaasConfig = System.getenv("FLINK_SASL_JAAS_CONFIG");
        if (saslJaasConfig == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please ensure env: FLINK_SASL_JAAS_CONFIG is set in application config").build();
        } 
        



        // Set the Flink properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("client.id", clientId);
        properties.put("sasl.mechanism", saslMechanism);
        properties.put("security.protocol", securityProtocol);
        properties.put("sasl.jaas.config", saslJaasConfig);

        // Parse topic parameter
        String qTopic = request.getQueryParameters().get("topic");
        String topic = request.getBody().orElse(qTopic);
        if (topic == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please pass a topic on the query string or in the request body").build();
        } 

         // Parse messagePrefix parameter
         String qMessagePrefix = request.getQueryParameters().get("messageprefix");
         String messagePrefix = request.getBody().orElse(qMessagePrefix);
         if (messagePrefix == null) {
             return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please pass a messageprefix on the query string or in the request body").build();
         } 

         // Parse numMessage parameter
         String qNumMessageStr = request.getQueryParameters().get("nummessage");
         String numMessageStr = request.getBody().orElse(qNumMessageStr);
         if (numMessageStr == null) {
             return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please pass a nummessage on the query string or in the request body").build();
         } 
         int numMessages = 0;
         try
         {
            numMessages = Integer.valueOf(numMessageStr);
         }
         catch (NumberFormatException nfe)
         {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Unable to parse nummessage on the query string or in the request body").build();
         }
         finally{}
         if (numMessages == 0)
         {
            return request.createResponseBuilder(HttpStatus.OK).body("Number of Messages was zero.  There's nothing to do!").build();
         }

         
         // Write out the parameters to the log
         context.getLogger().info("topic is " + topic);
         context.getLogger().info("message prefix is " + messagePrefix);
         context.getLogger().info("number of messages is " + numMessages);

         // create a Flink stream executin environment
         final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

         DataStream stream = createStream(env, messagePrefix, numMessages);
         
         FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(
            topic,
            new SimpleStringSchema(),   // serialization schema
            properties);

         stream.addSink(myProducer);
         try
         {
            env.execute("Sending batch to Kafka");
         }
         catch (Exception e)
         {
            context.getLogger().log(Level.SEVERE, "Exception occurred" + e);
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Exception occurred " + e).build();
         }

         return request.createResponseBuilder(HttpStatus.OK).body("Sucessfully sent " + numMessages + " messages to topic " + topic).build();

    }

    public static DataStream createStream(StreamExecutionEnvironment env, final String messagePrefix, final int numMessages){
        return env.generateSequence(0, numMessages)
            .map(new MapFunction<Long, String>() {
                @Override
                public String map(Long in) {
                    return messagePrefix + " " + in;
                }
            });
    }

}
