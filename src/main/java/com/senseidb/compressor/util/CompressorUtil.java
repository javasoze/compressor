package com.senseidb.compressor.util;

public class CompressorUtil {
  public static int getNumBits(long val) {
    int count = 0;
    while (val > 0) {
      count++;
      val = val >> 1;
    }
    return count;
  }
}
