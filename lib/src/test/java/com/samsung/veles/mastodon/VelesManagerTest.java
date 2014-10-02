package com.samsung.veles.mastodon;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
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

import com.samsung.veles.mastodon.VelesManager.Compression;
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

  public void testRouterDealer() {
    ZMQ.Context context = ZMQ.context(1);

    ZMQ.Socket in = context.socket(ZMQ.ROUTER);
    in.bind("ipc:///tmp/reqrep");

    ZMQ.Socket out = context.socket(ZMQ.DEALER);
    out.connect("ipc:///tmp/reqrep");

    for (int i = 0; i < 10; i++) {
      byte[] req = ("request" + i).getBytes();
      byte[] rep = ("reply" + i).getBytes();

      assertTrue(out.send(req, ZMQ.NOBLOCK));
      byte[] reqTmp = in.recv(0);
      byte[] reqTmp2 = in.recv(0);
      assertTrue(Arrays.equals(req, reqTmp2));

      in.send(reqTmp, ZMQ.SNDMORE);
      in.send(rep, 0);
      byte[] repTmp = out.recv();
      assertTrue(Arrays.equals(rep, repTmp));
    }
    in.close();
    out.close();
  }

  public String getUniqueFileName(String part) throws IOException {
    File f = File.createTempFile("mastodon-test-", "-".concat(part));
    String filename = f.getName();
    f.delete();
    return filename;
  }

  public class Receiver {
    private final ZMQInputStream _in;
    private final ZMQ.Socket _socket;
    private int _length;
    private byte[] _data;
    private byte[] _end;
    int _buffer_size = 4;
    int _capacity;

    public Receiver(ZMQ.Socket socket, int capacity) throws NoSuchFieldException,
        SecurityException, IllegalArgumentException, IllegalAccessException {
      _in = new ZMQInputStream(socket);
      _socket = socket;
      _capacity = capacity;
      Field field = ZMQOutputStream.class.getDeclaredField("PICKLE_END");
      field.setAccessible(true);
      _end = (byte[]) field.get(ZMQOutputStream.class);
    }

    public byte[] data() {
      // The length of the service information equals to 5
      return Arrays.copyOfRange(_data, 5, _length);
    }

    private boolean isEnded() {
      for (int i = 1; i <= _end.length; i++) {
        if (_data[_length - i] != _end[_end.length - i]) {
          return false;
        }
      }
      return true;
    }

    int getBufferSize() {
      return _buffer_size;
    }

    public void setBufferSize(int value) {
      _buffer_size = value;
    }

    public void receive() {
      int read = _buffer_size;
      _data = new byte[_capacity];
      _length = 0;
      do {
        read = _in.read(_data, _length, _buffer_size);
        _length += read;
      } while (!isEnded());
      _length -= _end.length;
      log.debug("done, retcode ".concat(Integer.toString(_length)));
    }

    public void reply(byte[] message) {
      _socket.send(_data, 0, 5, ZMQ.NOBLOCK | ZMQ.SNDMORE);
      _socket.send(message, ZMQ.NOBLOCK);
    }
  }

  public void testOpenStreams() throws InterruptedException, IllegalArgumentException,
      IllegalAccessException, NoSuchFieldException, SecurityException, NoSuchMethodException,
      InvocationTargetException, IOException {
    ZmqEndpoint endpoint =
        new ZmqEndpoint("localhost", "ipc", "ipc://".concat(getUniqueFileName("open-streams.ipc")));
    Field field = VelesManager.class.getDeclaredField("_context");
    field.setAccessible(true);
    ZMQ.Context context = (ZMQ.Context) field.get(VelesManager.instance());
    ZMQ.Socket socket = context.socket(ZMQ.ROUTER);
    socket.bind(endpoint.uri);

    field = VelesManager.class.getDeclaredField("_currentEndpoint");
    field.setAccessible(true);
    field.set(VelesManager.instance(), endpoint);

    Method method = VelesManager.class.getDeclaredMethod("openStreams");
    method.setAccessible(true);
    method.invoke(VelesManager.instance());

    field = VelesManager.class.getDeclaredField("_in");
    field.setAccessible(true);
    ZMQInputStream in = (ZMQInputStream) field.get(VelesManager.instance());
    Receiver receiver = new Receiver(socket, 128);

    field = VelesManager.class.getDeclaredField("_out");
    field.setAccessible(true);
    ZMQOutputStream out = (ZMQOutputStream) field.get(VelesManager.instance());

    String testMsg[] = {"test data", "a bit more data"};
    log.debug(String.format("sending data: \"%s\" + \"%s\"", testMsg[0], testMsg[1]));
    int[] bufferSizes = new int[] {4, 5, 64};

    for (int bufferSize : bufferSizes) {
      receiver.setBufferSize(bufferSize);
      log.debug(String.format("trying buffer size %d", bufferSize));
      out.write(testMsg[0].getBytes());
      out.write(testMsg[1].getBytes());
      out.close();
      receiver.receive();
      String recvMsg = new String(receiver.data());
      log.debug(String.format("received %d bytes: %s", recvMsg.length(), recvMsg));
      assertEquals(testMsg[0].concat(testMsg[1]), recvMsg);
    }
    receiver.reply(testMsg[1].getBytes());
    byte[] buffer = new byte[128];
    in.read(buffer);
    assertEquals(testMsg[1], new String(buffer).substring(0, testMsg[1].length()));

    socket.close();
    new File(endpoint.uri.substring(6)).delete();
  }

  private Object getTestObject() {
    TreeMap<String, Object> map = new TreeMap<>();
    map.put("Bruce", "Willis");
    map.put("Arnold", "Schwarzenegger");
    map.put("Jackie", "Chan");
    map.put("Sylvester", "Stallone");
    map.put("Chuck", "Norris");
    map.put("Array", new float[] {1, 2, 3});
    return map;
  }

  private void validateTestObject(Object back) {
    assertTrue(back instanceof Map);
    Map map_back = (Map) back;
    assertEquals(map_back.get("Bruce"), "Willis");
    assertTrue(map_back.get("Array") instanceof float[]);
    float[] arr = (float[]) map_back.get("Array");
    assertSame(arr.length, 3);
    assertTrue((arr[1] - 2) * (arr[1] - 2) < 0.000001);
  }

  public void testPickling() throws PickleException, IOException {
    Pickler pickler = new Pickler();
    Unpickler unpickler = new Unpickler();
    Object map = getTestObject();
    byte[] data = pickler.dumps(map);
    log.debug(String.format("Pickle took %d bytes", data.length));
    Object back = unpickler.loads(data);
    validateTestObject(back);
  }

  public void testExecutePickling() throws PickleException, IllegalAccessException,
      IllegalArgumentException, InvocationTargetException, IOException, NoSuchMethodException,
      SecurityException, NoSuchFieldException {
    Method submit = VelesManager.class.getDeclaredMethod("submit", Object.class, Compression.class);
    submit.setAccessible(true);
    Method yield = VelesManager.class.getDeclaredMethod("yield");
    yield.setAccessible(true);
    Field in = VelesManager.class.getDeclaredField("_in");
    in.setAccessible(true);
    Field out = VelesManager.class.getDeclaredField("_out");
    out.setAccessible(true);


    Object job = getTestObject();

    for (VelesManager.Compression codec : VelesManager.Compression.values()) {
      ByteArrayOutputStream fake_out = new ByteArrayOutputStream();
      out.set(VelesManager.instance(), fake_out);
      submit.invoke(VelesManager.instance(), job, codec);
      byte[] ser = fake_out.toByteArray();
      log.debug(String.format("Codec %s yielded %d bytes", codec.name(), ser.length));
      in.set(VelesManager.instance(), new ByteArrayInputStream(ser));
      Object res = yield.invoke(VelesManager.instance());
      validateTestObject(res);
    }
  }

  public class TestServer implements Runnable {
    private final ZMQ.Socket _socket;

    public TestServer(ZmqEndpoint endpoint) {
      ZMQ.Context context = ZMQ.context(1);
      _socket = context.socket(ZMQ.ROUTER);
      _socket.bind(endpoint.uri);
    }

    public void dispose() {
      _socket.close();
    }

    @Override
    public void run() {
      log.debug("working");
      ArrayList<byte[]> incoming = new ArrayList<>();
      boolean first = true;
      do {
        incoming.add(_socket.recv());
        if (first) {
          first = false;
          log.debug("receiving the message parts");
        }
      } while (_socket.hasReceiveMore());
      int overall = 0;
      for (int i = 1; i < incoming.size(); i++) {
        overall += incoming.get(i).length;
      }
      byte[] merged = new byte[overall];
      overall = 0;
      for (int i = 1; i < incoming.size(); i++) {
        int length = incoming.get(i).length;
        System.arraycopy(incoming.get(i), 0, merged, overall, length);
        overall += length;
      }
      log.debug(String.format("sending back the same pickle (%d bytes)", overall));
      _socket.send(incoming.get(0), ZMQ.NOBLOCK | ZMQ.SNDMORE);
      _socket.send(merged, ZMQ.NOBLOCK);
    }
  }

  public void testExecute() throws IOException, NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException, NoSuchMethodException,
      InvocationTargetException, InterruptedException, UnsupportedObjectException {
    ZmqEndpoint endpoint =
        new ZmqEndpoint("localhost", "ipc", "ipc://".concat(getUniqueFileName("execute.ipc")));
    TestServer server = new TestServer(endpoint);
    Thread t = new Thread(server);
    t.start();

    Field field = VelesManager.class.getDeclaredField("_currentEndpoint");
    field.setAccessible(true);
    field.set(VelesManager.instance(), endpoint);

    Method method = VelesManager.class.getDeclaredMethod("openStreams");
    method.setAccessible(true);
    method.invoke(VelesManager.instance());

    Object job = getTestObject();
    Object response = VelesManager.instance().execute(job);
    validateTestObject(response);
    t.join();
    server.dispose();
    new File(endpoint.uri.substring(6)).delete();
  }
}
