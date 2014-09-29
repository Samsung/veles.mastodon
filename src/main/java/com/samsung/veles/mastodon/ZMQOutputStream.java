package com.samsung.veles.mastodon;

import java.io.IOException;
import java.io.OutputStream;

import org.zeromq.ZMQ;

public class ZMQOutputStream extends OutputStream {
  private ZMQ.Socket _socket;
  
  public ZMQOutputStream(ZMQ.Socket socket) {
    _socket = socket;
  }

  @Override
  public void write(int b) throws IOException {
    byte[] msg = new byte[] { (byte)b };
    write(msg, 0, 1);
  }
  
  @Override
  public void write(byte[] b) {
    write(b, 0, b.length);   
  }
  
  @Override
  public void write(byte[] b, int off, int len) {
    _socket.send(b, off, len, ZMQ.NOBLOCK | ZMQ.SNDMORE);    
  }
  
  @Override
  public void close() {
    _socket.close();
  }
  
  public void finish() {
    byte[] end_marker = new byte[] { 0 };
    _socket.send(end_marker, 0);
  }
}
