package com.samsung.veles.mastodon;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.zeromq.ZMQ;

import com.samsung.veles.mastodon.VelesManager.ZmqEndpoint;

/**
 * Unit test for VelesManager.
 */
public class VelesManagerTest extends TestCase {

  static Logger log = Logger.getLogger(VelesManagerTest.class.getName());

  /**
   * Create the test case
   *
   * @param testName name of the test case
   */
  public VelesManagerTest(String testName) {
    super(testName);
    BasicConfigurator.configure();
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
    // get VelesManager.updateZmqEndpoints() method
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

    // get updated VelesManager field _endpoints
    Field field = null;
    try {
      field = VelesManager.class.getDeclaredField("_endpoints");
    } catch (NoSuchFieldException | SecurityException e3) {
      fail(e3.getMessage());
    }
    field.setAccessible(true);

    // check endpoints
    Map<String, List<ZmqEndpoint>> endpoints = null;
    try {
      endpoints =
          (Map<String, List<ZmqEndpoint>>) field.get(VelesManager.instance());
    } catch (IllegalArgumentException | IllegalAccessException e4) {
      fail(e4.getMessage());
    }
    assertEquals(1, endpoints.size());
    assertTrue(endpoints.containsKey("fd8e0fc6-b015-4245-922d-950dea3ac198"));
    List<ZmqEndpoint> list = endpoints.get(
        "fd8e0fc6-b015-4245-922d-950dea3ac198");
    assertEquals(3, list.size());
    ZmqEndpoint goldEndpoint = new ZmqEndpoint("markovtsevu64", "ipc",
        "ipc:///tmp/veles-ipc-zmqloader-dnioqryd");
    assertEquals(goldEndpoint, list.get(0));
    goldEndpoint.type = "tcp";
    goldEndpoint.uri = "tcp://markovtsevu64:52937";
    assertEquals(goldEndpoint, list.get(1));
    goldEndpoint.type = "inproc";
    goldEndpoint.uri = "inproc://veles-zmqloader-ZeroMQLoader";
    assertEquals(goldEndpoint, list.get(2));
  }

  private final String _sendingData = "test data";

  public class Receiver implements Runnable {
    public String receivedData;

    @Override
    public void run() {
      Field field = null;
      try {
        field = VelesManager.class.getDeclaredField("_in");
      } catch (NoSuchFieldException | SecurityException e) {
        fail(e.getMessage());
      }
      field.setAccessible(true);
      ZeroMQInputStream in = null;
      try {
        in = (ZeroMQInputStream) field.get(VelesManager.instance());
      } catch (IllegalArgumentException | IllegalAccessException e) {
        fail(e.getMessage());
      }

      log.debug("start recieving...");
      byte[] buffer = new byte[_sendingData.length()];
      in.read(buffer, 0, _sendingData.length());
      receivedData = new String(buffer);
      log.debug(String.format("recieved data: %s\n", receivedData));
    }
  }

  public void testOpenStreams() throws InterruptedException {
    Field field = null;
    ZMQ.Context context = ZMQ.context(1);
    ZMQ.Socket sock = context.socket(ZMQ.ROUTER);
    ZmqEndpoint endpoint = new ZmqEndpoint("markovtsevu64", "ipc",
        "ipc://VelesManager-test.ipc");
    sock.bind(endpoint.uri);

    try {
      field = VelesManager.class.getDeclaredField("_currentEndpoint");
    } catch (NoSuchFieldException | SecurityException e) {
      fail(e.getMessage());
    }
    field.setAccessible(true);
    try {
      field.set(VelesManager.instance(), endpoint);
    } catch (IllegalArgumentException | IllegalAccessException e) {
      fail(e.getMessage());
    }

    Method method = null;
    try {
      method = VelesManager.class.getDeclaredMethod("openStreams");
    } catch (NoSuchMethodException | SecurityException e) {
      fail(e.getMessage());
    }
    method.setAccessible(true);
    try {
      method.invoke(VelesManager.instance());
    } catch (IllegalAccessException | IllegalArgumentException |
        InvocationTargetException e) {
      fail(e.getMessage());
    }

    Receiver rec = new Receiver();
    Thread t = new Thread(rec);
    t.start();
    Thread.sleep(1000);

    log.debug(String.format("sending data: %s", _sendingData));
    sock.send("Mastodon".getBytes(), ZMQ.SNDMORE);
    sock.send(_sendingData, 0);
    t.join();
    assertEquals(_sendingData, rec.receivedData);
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
