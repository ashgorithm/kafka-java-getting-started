package org.example.errors.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.example.errors.events.InventoryEvent;

/**
 * JSON Serializer for Inventory Event
 */
public class InventoryEventSerializer implements Serializer<InventoryEvent> {
    private final ObjectMapper objectMapper = new ObjectMapper();

     @Override
    public byte[] serialize(String topic, InventoryEvent data) {
        try {
            if (data == null){
                System.out.println("Null received at serializing");
                return null;
            }
            System.out.println("Serializing...");
            return objectMapper.writeValueAsString(data).getBytes();
        } catch (Exception e) {
            throw new SerializationException("Error when serializing MessageDto to byte[]");
        }
    }

    @Override
    public void close() {
    }
}
