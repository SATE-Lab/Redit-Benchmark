/**
 * Autogenerated by Thrift Compiler (0.14.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hbase.thrift2.generated;


/**
 * Thrift wrapper around
 * org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
 */
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.14.1)", date = "2021-03-24")
public enum TDataBlockEncoding implements org.apache.thrift.TEnum {
  /**
   * Disable data block encoding.
   */
  NONE(0),
  PREFIX(2),
  DIFF(3),
  FAST_DIFF(4),
  ROW_INDEX_V1(7);

  private final int value;

  private TDataBlockEncoding(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  @org.apache.thrift.annotation.Nullable
  public static TDataBlockEncoding findByValue(int value) { 
    switch (value) {
      case 0:
        return NONE;
      case 2:
        return PREFIX;
      case 3:
        return DIFF;
      case 4:
        return FAST_DIFF;
      case 7:
        return ROW_INDEX_V1;
      default:
        return null;
    }
  }
}
