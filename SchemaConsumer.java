import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import org.slf4j.LoggerFactory; 
import org.slf4j.Logger;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.client.SchemaVersionInfoCache;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;


public class SchemaConsumer {
public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(SchemaConsumer.class);
        String schema_url = "<schema-registry-url>";
        Properties props = new Properties();

        //adjust properties according to consumer group id, commit etc...
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "bootstrap-servers");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName()); 
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-consumer10");
        props.putAll(Collections.singletonMap(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), schema_url));

        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
            try {
                consumer.subscribe(Arrays.asList("Avro"));
            // consume messages
                while(true){
                logger.debug("Consumer was successfully assigned to topic partition" + consumer.assignment());
                ConsumerRecords<String, GenericRecord> records  = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, GenericRecord> record : records) {
                        try {
                        logger.info("Received message: ({}, {}) at partition [{}-{}], offset [{}], with headers: {}", record.key(), record.value().toString(), record.topic(), record.partition(), record.offset(), Arrays.toString(record.headers().toArray()));
                        } catch (ClassCastException e) {
                        logger.warn("Could not deserialize message partition [{}-{}], offset [{}], with headers: {}", record.topic(), record.partition(), record.offset(), Arrays.toString(record.headers().toArray()));
                }
            }
        }
        }
        finally{
        consumer.close();
    }


}
}
