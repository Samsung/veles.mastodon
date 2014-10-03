package com.samsung.veles.mastodon;


public class SameHostMetrics implements EndpointMetrics {

  @Override
  public float distance(ZMQEndpoint endpoint, String localhost) {
    if (!endpoint.host.equals(localhost)) {
      if (endpoint.type.equals("tcp")) {
        return 1.f;
      }
      // other types are unacceptable
      return 2.f;
    }
    if (endpoint.type.equals("inproc")) {
      // Python and Java can not coexist in one process
      return 2.f;
    }
    if (endpoint.type.equals("ipc")) {
      return 0.f;
    }
    return 0.5f;
  }

}
