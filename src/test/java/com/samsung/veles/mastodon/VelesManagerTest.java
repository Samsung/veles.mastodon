package com.samsung.veles.mastodon;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

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
  @SuppressWarnings("unchecked")
  public void testUpdateZmqEndpoints() throws UnsupportedEncodingException {
    byte[] json = ("{\"fd8e0fc6-b015-4245-922d-950dea3ac198\": "
        + "{\"mid\": \"9d88104eccced2100a4a3ee851f7e8b0-c8600098e75f\", "
        + "\"pid\": 25766, \"host\": \"markovtsevu64\", \"power\": 100, "
        + "\"id\": \"fd8e0fc6-b015-4245-922d-950dea3ac198\", \"state\": "
        + "\"Working\", \"data\": [null, null, {\"ZmqLoaderEndpoints\": "
        + "{\"inproc\": [\"connect\", "
        + "\"inproc://veles-zmqloader-ZeroMQLoader\"], \"tcp\": "
        + "[\"connect\", \"tcp://*:52937\"], \"ipc\": [\"connect\", "
        + "\"ipc:///tmp/veles-ipc-zmqloader-dnioqryd\"]}}, null]}}").getBytes();
    Method method = null;
    try {
      method = VelesManager.class.getDeclaredMethod("updateZmqEndpoints",
          byte[].class);
    } catch (NoSuchMethodException | SecurityException e1) {
      fail(e1.getMessage());
    }
    method.setAccessible(true);
    try {
      method.invoke(VelesManager.instance(), json);
    } catch (IllegalAccessException | IllegalArgumentException |
        InvocationTargetException e2) {
      fail(e2.getMessage());
    }
    Field field = null;
    try {
      field = VelesManager.class.getDeclaredField("_endpoints");
    } catch (NoSuchFieldException | SecurityException e3) {
      fail(e3.getMessage());
    }
    field.setAccessible(true);
    Map<String, Map<String, String>> endpoints = null;
    try {
      endpoints =
          (Map<String, Map<String, String>>) field.get(VelesManager.instance());
    } catch (IllegalArgumentException | IllegalAccessException e4) {
      fail(e4.getMessage());
    }
    assertEquals(1, endpoints.size());
    assertTrue(endpoints.containsKey("fd8e0fc6-b015-4245-922d-950dea3ac198"));
    Map<String, String> val = endpoints.get(
        "fd8e0fc6-b015-4245-922d-950dea3ac198");
    assertEquals(3, val.size());
    assertTrue(val.containsKey("inproc"));
    assertTrue(val.containsKey("ipc"));
    assertTrue(val.containsKey("tcp"));
    String ep = val.get("inproc");
    assertEquals("inproc://veles-zmqloader-ZeroMQLoader", ep);
    ep = val.get("ipc");
    assertEquals("ipc:///tmp/veles-ipc-zmqloader-dnioqryd", ep);
    ep = val.get("tcp");
    assertEquals("tcp://markovtsevu64:52937", ep);
  }

  public void testChecksum() throws IOException {
    File tmp = File.createTempFile("mastodon-test-", ".txt");
    PrintWriter writer = new PrintWriter(tmp.getAbsolutePath(), "ASCII");
    writer.println("test text to check VelesManager.checksum()");
    writer.close();
    String cs = null;
    try {
      cs = VelesManager.checksum(tmp.getAbsolutePath());
    } catch (NoSuchAlgorithmException e) {
      fail(e.getMessage());
    }
    tmp.delete();
    assertEquals("2fbb51403bc48c145de6febf39193e42c34ef846", cs);
  }
}
