package com.shopkick.data.dataflow;

import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.StandardCharsets;

public class StringToBytesSchema implements DeserializationSchema<String>,
    SerializationSchema<String, byte[]> {

  private static final long serialVersionUID = 1L;

  @Override
  public String deserialize(byte[] message) {
    return new String(message);
  }

  @Override
  public boolean isEndOfStream(String nextElement) {
    return false;
  }

  @Override
  public byte[] serialize(String element) {
    return element.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public TypeInformation<String> getProducedType() {
    return BasicTypeInfo.STRING_TYPE_INFO;
  }
}