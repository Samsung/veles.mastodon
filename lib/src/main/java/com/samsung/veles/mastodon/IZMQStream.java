package com.samsung.veles.mastodon;

import org.zeromq.ZMQ;

public interface IZMQStream {
  public ZMQ.Socket getSocket();
}
