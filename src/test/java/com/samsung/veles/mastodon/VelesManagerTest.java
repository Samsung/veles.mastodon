package com.samsung.veles.mastodon;

import java.io.UnsupportedEncodingException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.alibaba.fastjson.JSON;

/**
 * Unit test for VelesManager.
 */
public class VelesManagerTest extends TestCase {
  /**
   * Create the test case
   *
   * @param testName name of the test case
   */
  public VelesManagerTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(VelesManagerTest.class);
  }

  /**
   * Rigorous Test :-)
   *
   * @throws UnsupportedEncodingException
   */
  public void testApp() throws UnsupportedEncodingException {
    JSON.parse("{\"fd8e0fc6-b015-4245-922d-950dea3ac198\": "
        + "{\"mid\": \"9d88104eccced2100a4a3ee851f7e8b0-c8600098e75f\", "
        + "\"pid\": 25766, \"host\": \"markovtsevu64\", \"power\": 100, "
        + "\"id\": \"fd8e0fc6-b015-4245-922d-950dea3ac198\", \"state\": "
        + "\"Working\", \"data\": [null, null, {\"ZmqLoaderEndpoints\": "
        + "{\"inproc\": [\"connect\", "
        + "\"inproc://veles-zmqloader-ZeroMQLoader\"], \"tcp\": "
        + "[\"connect\", \"tcp://*:52937\"], \"ipc\": [\"connect\", "
        + "\"ipc:///tmp/veles-ipc-zmqloader-dnioqryd\"]}}, null]}}");
  }
}
