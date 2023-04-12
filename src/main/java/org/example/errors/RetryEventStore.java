package org.example.errors;

import java.util.*;

/**
 * In memory store is required to keep track of what event or item is in retry topic
 * So that main app will push all related events to the retry topic to maintain order
 * Used a Map with item id as key. all the related events having same item id as value
 *
 */
public class RetryEventStore {
    private Map<Integer, List<Integer>> store = new HashMap<>();

    public void addEvent(int groupId, int eventId){
        if (!store.containsKey(groupId)){
            store.put(groupId, new ArrayList<Integer>());
        }
        List<Integer> relatedEvents = store.get(groupId);
        if (relatedEvents != null){
            relatedEvents.add(eventId);
        }
    }

    public boolean isRelatedEventRetried(int groupId){
        if (store.containsKey(groupId)){
            List<Integer> relatedEvents = store.get(groupId);
            if (relatedEvents != null && relatedEvents.size() > 0){
                return true;
            }
            else{
                return false;
            }

        }
        else{
            return false;
        }
    }

    public void removeEvent(int eventId){
        for (var entry : store.entrySet()){
            List<Integer> relatedEvents = entry.getValue();
            if(relatedEvents.remove(Integer.valueOf(eventId)));
        }
    }

    @Override
    public String toString() {
        return store.toString();
    }
}
