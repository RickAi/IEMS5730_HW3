package twitter.tools;

import java.io.Serializable;
import java.util.Map;

// This code was refers from the apache/storm storm-starter:
// https://github.com/apache/storm/blob/master/examples/storm-starter/src/jvm/org/apache/storm/starter/tools/SlidingWindowCounter.java

public final class SlidingWindowCounter<T> implements Serializable {

    private static final long serialVersionUID = -2645063988768785810L;

    private SlotBasedCounter<T> objCounter;
    private int headSlot;
    private int tailSlot;
    private int windowLengthInSlots;

    public SlidingWindowCounter(int windowLengthInSlots) {
        if (windowLengthInSlots < 2) {
            throw new IllegalArgumentException(
                    "Window length in slots must be at least two (you requested " + windowLengthInSlots + ")");
        }
        this.windowLengthInSlots = windowLengthInSlots;
        this.objCounter = new SlotBasedCounter<T>(this.windowLengthInSlots);

        this.headSlot = 0;
        this.tailSlot = slotAfter(headSlot);
    }

    public void incrementCount(T obj) {
        objCounter.incrementCount(obj, headSlot);
    }

    /**
     * Return the current (total) counts of all tracked objects, then advance the window.
     * <p/>
     * Whenever this method is called, we consider the counts of the current sliding window to be available to and
     * successfully processed "upstream" (i.e. by the caller). Knowing this we will start counting any subsequent
     * objects within the next "chunk" of the sliding window.
     *
     * @return The current (total) counts of all tracked objects.
     */
    public Map<T, Long> getCountsThenAdvanceWindow() {
        Map<T, Long> counts = objCounter.getCounts();
        objCounter.wipeZeros();
        objCounter.wipeSlot(tailSlot);
        advanceHead();
        return counts;
    }

    private void advanceHead() {
        headSlot = tailSlot;
        tailSlot = slotAfter(tailSlot);
    }

    private int slotAfter(int slot) {
        return (slot + 1) % windowLengthInSlots;
    }

}
