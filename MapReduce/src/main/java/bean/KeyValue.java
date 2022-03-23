package bean;

public class KeyValue implements Comparable<KeyValue>{
    private String key;
    private String value;

    public KeyValue(String Key, String Value) {
        this.key = Key;
        this.value = Value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
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
