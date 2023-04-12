package org.example.errors.events;

/**
 * After an event is sent to Retry topic, a corresponding event called Redirect Event is sent to Redirect Topic. *
 * When the Retry App processes an event, it publishes a tombstone event to Redirect App
 * Redirect topic is subscribed by the main app for any tombstone events.
 * It updates the in memory store which is used to look up any retried items present.
 */
public class RedirectEvent {
    public int eventId;
    public boolean isTombstone;

    @Override
    public String toString() {
        return "RedirectEvent{" +
                "eventId=" + eventId +
                ", isTombstone=" + isTombstone +
                '}';
    }
}
