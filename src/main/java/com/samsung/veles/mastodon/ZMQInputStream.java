package com.samsung.veles.mastodon;

import java.io.IOException;
import java.io.InputStream;

import org.zeromq.ZMQ;

public class ZMQInputStream extends InputStream {
private final ZMQ.Socket _socket;

  public ZMQInputStream(ZMQ.Socket socket) {
    _socket = socket;
  }

  @Override
  public int read() throws IOException {
    byte[] buf = new byte[] { 0 };
    int res = read(buf);
    if (res == -1) {
      return -1;
    }
    return buf[0];
  }

  @Override
  public int read(byte[] b) {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) {
    if (!_socket.hasReceiveMore()) {
      // current message is not a multi-part
      //TODO(seninds): add complex logic to receive multi-part message
      return _socket.recv(b, off, len, 0);
    } else {
      // current message is a multi-part
      return _socket.recv(b, off, len, 0);
    }
  }

  @Override
  public void close() {
    _socket.close();
  }

  @Override
  public int available() {
    if (!_socket.hasReceiveMore()) {
      return 0;
    }
    return 4;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  public byte[] readMsgPart() {
    return _socket.recv();
  }
}
