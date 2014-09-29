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
   * Tests ZeroMQ endpoints parsing.
   */
  @SuppressWarnings("unchecked")
  public void testUpdateZmqEndpoints() throws UnsupportedEncodingException, NoSuchMethodException,
      SecurityException, IllegalAccessException, IllegalArgumentException,
      InvocationTargetException, NoSuchFieldException {
    byte[] json =
        ("{\"fd8e0fc6-b015-4245-922d-950dea3ac198\": "
            + "{\"mid\": \"9d88104eccced2100a4a3ee851f7e8b0-c8600098e75f\", "
            + "\"pid\": 25766, \"host\": \"markovtsevu64\", \"power\": 100, "
            + "\"id\": \"fd8e0fc6-b015-4245-922d-950dea3ac198\", \"state\": "
            + "\"Working\", \"data\": [null, null, {\"ZmqLoaderEndpoints\": "
            + "{\"inproc\": [\"connect\", "
            + "\"inproc://veles-zmqloader-ZeroMQLoader\"], \"tcp\": "
            + "[\"connect\", \"tcp://*:52937\"], \"ipc\": [\"connect\", "
            + "\"ipc:///tmp/veles-ipc-zmqloader-dnioqryd\"]}}, null]}}").getBytes();

    // get VelesManager.updateZmqEndpoints() method
    Method method = VelesManager.class.getDeclaredMethod("updateZmqEndpoints", byte[].class);
    method.setAccessible(true);
    method.invoke(VelesManager.instance(), json);

    // get updated VelesManager field _endpoints
    Field field = VelesManager.class.getDeclaredField("_endpoints");
    field.setAccessible(true);

    // check endpoints
    Map<String, List<ZmqEndpoint>> endpoints =
        (Map<String, List<ZmqEndpoint>>) field.get(VelesManager.instance());
    assertEquals(1, endpoints.size());
    assertTrue(endpoints.containsKey("fd8e0fc6-b015-4245-922d-950dea3ac198"));
    List<ZmqEndpoint> list = endpoints.get("fd8e0fc6-b015-4245-922d-950dea3ac198");
    assertEquals(3, list.size());

    ZmqEndpoint goldEndpoint =
        new ZmqEndpoint("markovtsevu64", "ipc", "ipc:///tmp/veles-ipc-zmqloader-dnioqryd");
    assertEquals(goldEndpoint, list.get(0));
    goldEndpoint.type = "tcp";
    goldEndpoint.uri = "tcp://markovtsevu64:52937";
    assertEquals(goldEndpoint, list.get(1));
    goldEndpoint.type = "inproc";
    goldEndpoint.uri = "inproc://veles-zmqloader-ZeroMQLoader";
    assertEquals(goldEndpoint, list.get(2));
  }

  public class Receiver implements Runnable {
    private final ZMQInputStream _in;
    private byte[] _data;

    public Receiver(ZMQInputStream in) {
      _in = in;
    }

    public byte[] data() { return _data; }

    @Override
    public void run() {
      log.debug("server starts receiving...");
      _data = _in.readMsgPart();
      log.debug("done!");
    }
  }

  public void testOpenStreams() throws InterruptedException, IllegalArgumentException,
      IllegalAccessException, NoSuchFieldException, SecurityException, NoSuchMethodException,
      InvocationTargetException {
    ZmqEndpoint endpoint = new ZmqEndpoint("seninu64", "ipc", "ipc://open-streams.ipc");

    ZMQ.Context context = ZMQ.context(1);
    ZMQ.Socket socket = context.socket(ZMQ.ROUTER);
    socket.bind(endpoint.uri);

    Field field = VelesManager.class.getDeclaredField("_currentEndpoint");
    field.setAccessible(true);
    field.set(VelesManager.instance(), endpoint);
    Method method = VelesManager.class.getDeclaredMethod("openStreams");
    method.setAccessible(true);
    method.invoke(VelesManager.instance());
    field = VelesManager.class.getDeclaredField("_in");
    field.setAccessible(true);

    ZMQInputStream in = (ZMQInputStream) field.get(VelesManager.instance());
    Receiver receiver = new Receiver(in);
    Thread t = new Thread(receiver);
    t.start();

    String testMsg = "test data";
    log.debug(String.format("sending data: %s", testMsg));
    socket.send("Mastodon".getBytes(), ZMQ.SNDMORE);
    socket.send(testMsg.getBytes(), 0);
    t.join();

    String recvMsg = new String(receiver.data());
    log.debug(String.format("received data: (%d)", recvMsg.length()));
    assertEquals(testMsg, new String(receiver.data()));
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
