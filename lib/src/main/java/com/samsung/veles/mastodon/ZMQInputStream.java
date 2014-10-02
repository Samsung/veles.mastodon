package com.samsung.veles.mastodon;

import java.io.IOException;
import java.io.InputStream;

import org.zeromq.ZMQ;

public class ZMQInputStream extends InputStream {
  private final ZMQ.Socket _socket;
  private byte[] _unread;
  private int _unread_pos = 0;
  private boolean _new_message = true;
  private final byte[] _int_buf = new byte[1];

  public ZMQInputStream(ZMQ.Socket socket) {
    _socket = socket;
  }

  @Override
  public int read() throws IOException {
    int res = read(_int_buf);
    if (res == -1) {
      return -1;
    }
    return _int_buf[0];
  }

  @Override
  public int read(byte[] b) {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) {
    int pos = off;
    int pending = len;

    if (_unread != null) {
      int read = Math.min(_unread.length - _unread_pos, len);
      System.arraycopy(_unread, _unread_pos, b, pos, read);
      _unread_pos += read;
      boolean more = _unread_pos < _unread.length;
      if (!more) {
        _unread_pos = 0;
        _unread = null;
      }
      if (more || _new_message) {
        return read;
      }
      pos += read;
      pending -= read;
    }

    do {
      Object readobj = _socket.recv_rem(b, pos, pending, 0);
      _new_message = !_socket.hasReceiveMore();
      if (readobj instanceof byte[]) {
        _unread = (byte[]) readobj;
        return len;
      }
      int read = (int) readobj;
      pos += read;
      pending -= read;
    } while (!_new_message);

    return len - pending;
  }

  @Override
  public void close() {
    _unread_pos = 0;
    _unread = null;
    byte[] tmp = new byte[0];
    while (_socket.hasReceiveMore()) {
      _socket.recv(tmp, 0, 0, 0);
    }
  }

  @Override
  public int available() {
    return _unread != null ? _unread.length - _unread_pos : 0;
  }

  @Override
  public boolean markSupported() {
    return false;
  }
}
