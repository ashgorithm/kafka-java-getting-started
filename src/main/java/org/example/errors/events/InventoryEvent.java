package org.example.errors.events;
/**
 *The event from the source topic Inventory.
 * eventId: unique ID for the event
 * itemId: events belong to a particular group. here it is the item. all the events related to a particular item
 * will be queued together in the retry queue if any item event is in the retry queue.
 * change: how much inventory has changed we can keep a track using this
 *

 */
public class InventoryEvent {
    public int eventId;
    public int itemId;
    public String itemName;
    public int change;

    public InventoryEvent(){

    }

    public InventoryEvent(int eventId, int itemId, String itemName, int change) {
        this.eventId = eventId;
        this.itemId = itemId;
        this.itemName = itemName;
        this.change = change;
    }

    @Override
    public String toString() {
        return "InventoryEvent{" +
                "eventId=" + eventId +
                ", itemId=" + itemId +
                ", itemName='" + itemName + '\'' +
                ", change=" + change +
                '}';
    }
}
