package edu.fudan.stormcv.bolt;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import edu.fudan.stormcv.model.CVParticle;
import org.apache.storm.tuple.Fields;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 2/28/17 - 7:27 AM
 * Description:
 */

public class History implements Serializable {

    private Cache<CVParticle, String> inputCache;
    private HashMap<String, List<CVParticle>> groups;

    private int TTL = 29;
    private int maxSize = 256;
    private boolean refreshExperation = true;

    /**
     * Creates a History object for the specified Bolt (which is used to ack or fail items removed from the history).
     *
     * @param bolt
     */
    public History(RemovalListener<CVParticle, String> bolt) {
        groups = new HashMap<String, List<CVParticle>>();
        inputCache = CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .expireAfterAccess(TTL, TimeUnit.SECONDS) // resets also on get(...)!
                .removalListener(bolt)
                .build();
    }

    public History(RemovalListener<CVParticle, String> bolt, int TTL, int maxSize, boolean refreshExperation) {
        this.TTL = TTL;
        this.maxSize = maxSize;
        this.refreshExperation = refreshExperation;
        groups = new HashMap<String, List<CVParticle>>();
        inputCache = CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .expireAfterAccess(TTL, TimeUnit.SECONDS) // resets also on get(...)!
                .removalListener(bolt)
                .build();
    }

    /**
     * Adds the new CVParticle object to the history and returns the list of items it was grouped with.
     *
     * @param group    the name of the group the CVParticle belongs to
     * @param particle the CVParticle object that needs to be added to the history.
     */
    public void add(String group, CVParticle particle) {
        if (!groups.containsKey(group)) {
            groups.put(group, new ArrayList<CVParticle>());
        }
        List<CVParticle> list = groups.get(group);
        int i;
        for (i = list.size() - 1; i >= 0; i--) {
            if (particle.getSequenceNr() > list.get(i).getSequenceNr()) {
                list.add(i + 1, particle);
                break;
            }
            if (refreshExperation) {
                inputCache.getIfPresent(list.get(i)); // touch the item passed in the cache to reset its expiration timer
            }
        }
        if (i < 0) list.add(0, particle);
        inputCache.put(particle, group);
    }

    /**
     * Removes the object from the history. This will tricker an ACK to be send.
     *
     * @param particle
     */
    public void removeFromHistory(CVParticle particle) {
        inputCache.invalidate(particle);
    }

    /**
     * Removes the object from the group
     *
     * @param particle
     * @param group
     */
    public void clear(CVParticle particle, String group) {
        if (!groups.containsKey(group)) return;
        groups.get(group).remove(particle);
        if (groups.get(group).size() == 0) groups.remove(group);
    }

    /**
     * Returns all the items in this history that belong to the specified group
     *
     * @param group
     * @return
     */
    public List<CVParticle> getGroupedItems(String group) {
        return groups.get(group);
    }

    public long size() {
        return inputCache.size();
    }

    @Override
    public String toString() {
        String result = "";
        for (String group : groups.keySet()) {
            result += "  " + group + " : " + groups.get(group).size() + "\r\n";
        }
        return result;
    }
}// end of History class
