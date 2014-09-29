package com.samsung.veles.mastodon;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;
import net.razorvine.pickle.Unpickler;

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
    if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
      BasicConfigurator.configure();
    }
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

  public String getUniqueFileName(String part) throws IOException {
    File f = File.createTempFile("mastodon-test-", "-".concat(part));
    String filename = f.getName();
    f.delete();
    return filename;
  }

  public class Receiver implements Runnable {
    private final ZMQInputStream _in;
    private int _length;
    private byte[] _data;
    private byte[] _end;

    public Receiver(ZMQInputStream in, int buffer_size) throws NoSuchFieldException,
        SecurityException, IllegalArgumentException, IllegalAccessException {
      _in = in;
      _length = 0;
      _data = new byte[buffer_size];
      Field field = ZMQOutputStream.class.getDeclaredField("PICKLE_END");
      field.setAccessible(true);
      _end = (byte[]) field.get(ZMQOutputStream.class);
    }

    public byte[] data() {
      return Arrays.copyOfRange(_data, 0, _length);
    }

    private boolean isEnded() {
      for (int i = 1; i <= _end.length; i++) {
        if (_data[_length - i] != _end[_end.length - i]) {
          return false;
        }
      }
      return true;
    }

    @Override
    public void run() {
      log.debug("server starts receiving...");
      int buf_size = 4;
      int read = buf_size;
      do {
        read = _in.read(_data, _length, buf_size);
        _length += read;
      } while (!isEnded());
      _length -= _end.length;
      log.debug("done, retcode ".concat(Integer.toString(_length)));
    }
  }

  public void testOpenStreams() throws InterruptedException, IllegalArgumentException,
      IllegalAccessException, NoSuchFieldException, SecurityException, NoSuchMethodException,
      InvocationTargetException, IOException {
    ZmqEndpoint endpoint =
        new ZmqEndpoint("localhost", "ipc", "ipc://".concat(getUniqueFileName("open-streams.ipc")));

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
    Receiver receiver = new Receiver(in, 128);

    field = VelesManager.class.getDeclaredField("_out");
    field.setAccessible(true);
    ZMQOutputStream out = (ZMQOutputStream) field.get(VelesManager.instance());
    field = ZMQOutputStream.class.getDeclaredField("_socket");
    field.setAccessible(true);
    field.set(out, socket);

    String testMsg[] = {"test data", "a bit more data"};

    Thread t = new Thread(receiver);
    t.start();
    log.debug(String.format("sending data: \"%s\" + \"%s\"", testMsg[0], testMsg[1]));
    out.start();
    out.write(testMsg[0].getBytes());
    out.write(testMsg[1].getBytes());
    out.finish();
    t.join();

    String recvMsg = new String(receiver.data());
    log.debug(String.format("received %d bytes: %s", recvMsg.length(), recvMsg));
    assertEquals(testMsg[0].concat(testMsg[1]), recvMsg);
    new File(endpoint.uri.substring(6)).delete();
  }

  public void testPickling() throws PickleException, IOException {
    Pickler pickler = new Pickler();
    Unpickler unpickler = new Unpickler();
    TreeMap<String, Object> map = new TreeMap<>();
    map.put("Bruce", "Willis");
    map.put("Arnold", "Schwarzenegger");
    map.put("Jackie", "Chan");
    map.put("Sylvester", "Stallone");
    map.put("Chuck", "Norris");
    map.put("Array", new float[] {1, 2, 3});
    byte[] data = pickler.dumps(map);
    log.debug(String.format("Pickle took %d bytes", data.length));
    Object back = unpickler.loads(data);
    assertTrue(back instanceof Map);
    Map map_back = (Map) back;
    assertEquals(map_back.get("Bruce"), "Willis");
    assertTrue(map_back.get("Array") instanceof float[]);
    float[] arr = (float[]) map_back.get("Array");
    assertSame(arr.length, 3);
    assertTrue((arr[1] - 2) * (arr[1] - 2) < 0.000001);
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
