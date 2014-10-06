package com.samsung.veles.mastodon;


public class ZMQEndpoint {
  public ZMQEndpoint(String host, String type, String uri) {
    this.host = host;
    this.uri = uri;
    this.type = type;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other == this)
      return true;
    if (!(other instanceof ZMQEndpoint))
      return false;

    ZMQEndpoint endpoint = (ZMQEndpoint) other;
    if (this.host.equals(endpoint.host) && this.uri.equals(endpoint.uri)
        && this.type.equals(endpoint.type)) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * The remote host name.
   */
  public String host;
  /**
   * The full ZeroMQ endpoint string, e.g. tcp://192.168.0.1:2000.
   */
  public String uri;
  /**
   * The transport name, e.g. tcp or inproc.
   */
  public String type;

  @Override
  public String toString() {
    return String.format("(%s) %s", host, uri);
  }
}
