package com.senseidb.compressor.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.lucene.store.DataInput;

import com.senseidb.compressor.idset.MemoryAccessor;

public class DirectByteBufferDataInput extends DataInput {

  private final ByteBuffer buffer;
  private final long baseAddr;
  public DirectByteBufferDataInput(ByteBuffer buffer){
    if (buffer.isDirect()){
      this.buffer = buffer;
      baseAddr = MemoryAccessor.getDirectBufferBaseAddress(buffer);
    }
    else{
      throw new IllegalArgumentException("input buffer is not direct");
    }
  }
  
  @Override
  public short readShort() throws IOException {
    int pos = buffer.position();
    short val = MemoryAccessor.getShort(baseAddr+pos);
    buffer.position(pos+2);
    return val;
  }

  @Override
  public int readInt() throws IOException {
    int pos = buffer.position();
    int val = MemoryAccessor.getInt(baseAddr+pos);
    buffer.position(pos+4);
    return val;

   /* int pos = buffer.position();
    int val = buffer.getInt();
    
    int val2 = MemoryAccessor.getInt(baseAddr + pos);
    */
    
  }

  @Override
  public long readLong() throws IOException {
    int pos = buffer.position();
    long val = MemoryAccessor.getLong(baseAddr+pos);
    buffer.position(pos+8);
    return val;
  }

  @Override
  public byte readByte() throws IOException {
    int pos = buffer.position();
    byte val = (byte)(MemoryAccessor.getByte(baseAddr,pos));
    buffer.position(pos+1);
    return val;
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    int pos = buffer.position();
    for (int i=0;i<len;++i){
      b[offset+i]=(byte)MemoryAccessor.getByte(baseAddr, pos+i);
    }
    buffer.get(b, offset, len);
  }

}
