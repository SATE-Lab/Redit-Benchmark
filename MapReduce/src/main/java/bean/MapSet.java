package bean;

import java.util.HashMap;

public class MapSet {
    private HashMap<Object, Boolean> mapBool;
    private int count;

    public MapSet() {
        this.mapBool = new HashMap<Object, Boolean>();
        this.count = 0;
    }

    public HashMap<Object, Boolean> getMapBool() {
        return mapBool;
    }

    public int getCount() {
        return count;
    }

    /**
     * Insert a map task into the MapSet.
     * @param data
     */
    public void Insert(Object data){
        this.mapBool.put(data, true);
        this.count++;
    }

    /**
     * Verify that the element is included in the MapSet.
     * @param data
     * @return
     */
    public boolean Has(Object data){
        return this.mapBool.get(data);
    }

    /**
     * Remove a map task from the MapSet.
     * @param data
     */
    public void Remove(Object data){
        this.mapBool.remove(data);
        this.mapBool.put(data, false);
        this.count--;
    }

}

