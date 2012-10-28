package com.senseidb.compressor.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.lucene.store.DataOutput;

import com.senseidb.compressor.idset.MemoryAccessor;

public class DirectByteBufferDataOutput extends DataOutput {

  private final ByteBuffer buffer;
  private final long baseAddr;
  
  public DirectByteBufferDataOutput(ByteBuffer buffer){
    if (buffer.isDirect()){
      this.buffer = buffer;
      this.baseAddr = MemoryAccessor.getDirectBufferBaseAddress(buffer);
    }
    else{
      throw new IllegalArgumentException("buffer not direct");
    }
  }
  
  @Override
  public void writeInt(int i) throws IOException {
    int pos = buffer.position();
    MemoryAccessor.putInt(baseAddr+pos, i);
    buffer.position(pos+4);
  }

  @Override
  public void writeLong(long i) throws IOException {
    int pos = buffer.position();
    MemoryAccessor.putLong(baseAddr+pos, i);
    buffer.position(pos+8);
  }

  @Override
  public void writeByte(byte b) throws IOException {
    int pos = buffer.position();
    MemoryAccessor.putByte(baseAddr+pos, b);
    buffer.position(pos+1);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    int pos = buffer.position();
    for (int i=0;i<length;++i){
      MemoryAccessor.putByte(baseAddr+pos+i, b[offset+i]);
    }
    buffer.position(pos+length);
  }

}
