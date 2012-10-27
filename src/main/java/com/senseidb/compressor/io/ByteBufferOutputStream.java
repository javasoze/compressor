package com.senseidb.compressor.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ByteBufferOutputStream extends OutputStream {

  private final ByteBuffer byteBuffer;

  public ByteBufferOutputStream(ByteBuffer byteBuffer) {
    this.byteBuffer = byteBuffer;
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  @Override
  public void write(int b) throws IOException {
    byteBuffer.put((byte) b);
  }

  @Override
  public void write(byte[] bytes, int offset, int length) throws IOException {
    byteBuffer.put(bytes, offset, length);
  }

}
