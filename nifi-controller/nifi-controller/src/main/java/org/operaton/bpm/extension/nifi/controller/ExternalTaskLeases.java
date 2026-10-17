package org.operaton.bpm.extension.nifi.controller;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Locks held by this worker that are extended automatically until their external task is
 * finished. Updates are compare-and-set so that a task released while its lock is being
 * extended is not registered again.
 */
final class ExternalTaskLeases {

    /**
     * @param lockDuration lock duration to extend by, in milliseconds
     * @param lockedAt when the task was fetched and locked, in epoch milliseconds
     * @param expiresAt when the current lock expires, in epoch milliseconds
     */
    record Lease(long lockDuration, long lockedAt, long expiresAt) {

        /**
         * @return when the lock should be extended, i.e. half way through its duration
         */
        long renewAt() {
            return expiresAt - lockDuration / 2;
        }

        Lease expiringAt(long newExpiresAt) {
            return new Lease(lockDuration, lockedAt, newExpiresAt);
        }

        String serialize() {
            return lockDuration + "," + lockedAt + "," + expiresAt;
        }

        static Lease parse(String value) {
            String[] parts = value.split(",");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid lease: " + value);
            }
            return new Lease(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Long.parseLong(parts[2]));
        }
    }

    private final ConcurrentMap<String, Lease> leases = new ConcurrentHashMap<>();
    private final AtomicBoolean dirty = new AtomicBoolean();

    void register(String taskId, long lockDuration, long now) {
        leases.put(taskId, new Lease(lockDuration, now, now + lockDuration));
        dirty.set(true);
    }

    void release(String taskId) {
        if (leases.remove(taskId) != null) {
            dirty.set(true);
        }
    }

    boolean renew(String taskId, Lease lease, long newExpiresAt) {
        boolean renewed = leases.replace(taskId, lease, lease.expiringAt(newExpiresAt));
        if (renewed) {
            dirty.set(true);
        }
        return renewed;
    }

    boolean remove(String taskId, Lease lease) {
        boolean removed = leases.remove(taskId, lease);
        if (removed) {
            dirty.set(true);
        }
        return removed;
    }

    /**
     * @return leases whose locks should be extended by now, the soonest expiring first
     */
    List<Map.Entry<String, Lease>> due(long now) {
        return leases.entrySet().stream()
                .filter(entry -> entry.getValue().renewAt() <= now)
                .map(entry -> Map.entry(entry.getKey(), entry.getValue()))
                .sorted(Comparator.comparingLong(entry -> entry.getValue().expiresAt()))
                .toList();
    }

    Lease get(String taskId) {
        return leases.get(taskId);
    }

    int size() {
        return leases.size();
    }

    void clear() {
        leases.clear();
        dirty.set(false);
    }

    void markDirty() {
        dirty.set(true);
    }

    /**
     * @return whether leases changed since the last call
     */
    boolean takeDirty() {
        return dirty.getAndSet(false);
    }

    Map<String, String> toState() {
        Map<String, String> state = new HashMap<>();
        leases.forEach((taskId, lease) -> state.put(taskId, lease.serialize()));
        return state;
    }

    /**
     * Replace the leases with ones read from {@link #toState()}.
     *
     * @return the number of entries that could not be parsed and were skipped
     */
    int load(Map<String, String> state) {
        leases.clear();
        int invalid = 0;
        for (Map.Entry<String, String> entry : state.entrySet()) {
            try {
                leases.put(entry.getKey(), Lease.parse(entry.getValue()));
            } catch (IllegalArgumentException e) {
                invalid++;
            }
        }
        dirty.set(invalid > 0);
        return invalid;
    }
}
