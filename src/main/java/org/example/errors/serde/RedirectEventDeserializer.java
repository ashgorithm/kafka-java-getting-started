package org.example.errors.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.example.errors.events.RedirectEvent;

public class RedirectEventDeserializer implements Deserializer<RedirectEvent> {
    private ObjectMapper objectMapper = new ObjectMapper();


    @Override
    public RedirectEvent deserialize(String topic, byte[] data) {
        try {
            if (data == null){
                System.out.println("Null received at deserializing");
                return null;
            }
            System.out.println("Deserializing...");
            return objectMapper.readValue(new String(data, "UTF-8"), RedirectEvent.class);
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to InventoryEvent");
        }
    }

    @Override
    public void close() {
    }
}
