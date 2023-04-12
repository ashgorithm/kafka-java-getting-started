package org.example.errors.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.example.errors.events.InventoryEvent;

/**
 * JSON De Serializer for Inventory Event
 */
public class InventoryEventDeserializer implements Deserializer<InventoryEvent> {
    private ObjectMapper objectMapper = new ObjectMapper();


    @Override
    public InventoryEvent deserialize(String topic, byte[] data) {
        try {
            if (data == null){
                System.out.println("Null received at deserializing");
                return null;
            }
            System.out.println("Deserializing..." + data);
            return objectMapper.readValue(new String(data, "UTF-8"), InventoryEvent.class);
        } catch (Exception e) {
            System.out.println("Error when deserializing byte[] to InventoryEvent");
            return null;
//            throw new SerializationException("Error when deserializing byte[] to InventoryEvent");
        }
    }

    @Override
    public void close() {
    }
}
