package bean;

public class KeyValue implements Comparable<KeyValue>{
    private String key;
    private int value;

    public KeyValue(String Key, int Value) {
        this.key = Key;
        this.value = Value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        key = key;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        value = value;
    }

    @Override
    public String toString() {
        return "KeyValue{" +
                "Key='" + key + '\'' +
                ", Value=" + value +
                '}';
    }

    @Override
    public int compareTo(KeyValue o) {
        return this.key.compareTo(o.key);
    }
}
