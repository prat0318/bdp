/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.refactorlabs.cs378.wordcount;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class WordCountData extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"WordCountData\",\"namespace\":\"com.refactorlabs.cs378.wordcount\",\"fields\":[{\"name\":\"count\",\"type\":\"long\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public long count;

  /**
   * Default constructor.
   */
  public WordCountData() {}

  /**
   * All-args constructor.
   */
  public WordCountData(java.lang.Long count) {
    this.count = count;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return count;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: count = (java.lang.Long)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'count' field.
   */
  public java.lang.Long getCount() {
    return count;
  }

  /**
   * Sets the value of the 'count' field.
   * @param value the value to set.
   */
  public void setCount(java.lang.Long value) {
    this.count = value;
  }

  /** Creates a new WordCountData RecordBuilder */
  public static com.refactorlabs.cs378.wordcount.WordCountData.Builder newBuilder() {
    return new com.refactorlabs.cs378.wordcount.WordCountData.Builder();
  }
  
  /** Creates a new WordCountData RecordBuilder by copying an existing Builder */
  public static com.refactorlabs.cs378.wordcount.WordCountData.Builder newBuilder(com.refactorlabs.cs378.wordcount.WordCountData.Builder other) {
    return new com.refactorlabs.cs378.wordcount.WordCountData.Builder(other);
  }
  
  /** Creates a new WordCountData RecordBuilder by copying an existing WordCountData instance */
  public static com.refactorlabs.cs378.wordcount.WordCountData.Builder newBuilder(com.refactorlabs.cs378.wordcount.WordCountData other) {
    return new com.refactorlabs.cs378.wordcount.WordCountData.Builder(other);
  }
  
  /**
   * RecordBuilder for WordCountData instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<WordCountData>
    implements org.apache.avro.data.RecordBuilder<WordCountData> {

    private long count;

    /** Creates a new Builder */
    private Builder() {
      super(com.refactorlabs.cs378.wordcount.WordCountData.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.refactorlabs.cs378.wordcount.WordCountData.Builder other) {
      super(other);
    }
    
    /** Creates a Builder by copying an existing WordCountData instance */
    private Builder(com.refactorlabs.cs378.wordcount.WordCountData other) {
            super(com.refactorlabs.cs378.wordcount.WordCountData.SCHEMA$);
      if (isValidValue(fields()[0], other.count)) {
        this.count = data().deepCopy(fields()[0].schema(), other.count);
        fieldSetFlags()[0] = true;
      }
    }

    /** Gets the value of the 'count' field */
    public java.lang.Long getCount() {
      return count;
    }
    
    /** Sets the value of the 'count' field */
    public com.refactorlabs.cs378.wordcount.WordCountData.Builder setCount(long value) {
      validate(fields()[0], value);
      this.count = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'count' field has been set */
    public boolean hasCount() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'count' field */
    public com.refactorlabs.cs378.wordcount.WordCountData.Builder clearCount() {
      fieldSetFlags()[0] = false;
      return this;
    }

    @Override
    public WordCountData build() {
      try {
        WordCountData record = new WordCountData();
        record.count = fieldSetFlags()[0] ? this.count : (java.lang.Long) defaultValue(fields()[0]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
