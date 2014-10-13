package com.samsung.veles.mastodon;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;
import net.razorvine.pickle.Unpickler;

import org.apache.log4j.Logger;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;
import org.zeromq.ZMQ;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;


/**
 * Connects to Veles workflow's master and submits jobs.
 *
 */
public class VelesManager {
  private static volatile VelesManager _instance = null;

  public static VelesManager instance() {
    if (_instance == null) {
      synchronized (VelesManager.class) {
        if (_instance == null) {
          _instance = new VelesManager();
          Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
              for (Map.Entry<String, ZMQInputStream> pair : _instance._pending.entrySet()) {
                _instance.popJob(pair.getValue().getSocket(), pair.getKey());
              }
              if (_instance._socket != null) {
                _instance._socket.close();
              }
              _instance._context.term();
            }
          });
        }
      }
    }
    return _instance;
  }

  /**
   * Calculates the checksum of the file with Veles model. It can be passed in to
   * {@link #connect(String, int, String) connect()} as workflowId.
   *
   * @return String with SHA1 file hash.
   * @throws NoSuchAlgorithmException, IOException
   */
  public static String checksum(String fileName) throws NoSuchAlgorithmException, IOException {
    FileInputStream fis = new FileInputStream(fileName);
    BufferedInputStream bis = new BufferedInputStream(fis);
    MessageDigest sha1 = MessageDigest.getInstance("SHA1");
    DigestInputStream dis = new DigestInputStream(bis, sha1);

    try {
      while (dis.read() != -1);
    } finally {
      dis.close();
    }

    byte[] hash = sha1.digest();

    Formatter formatter = new Formatter();
    for (byte b : hash) {
      formatter.format("%02x", b);
    }
    String result = formatter.toString();
    formatter.close();

    return result;
  }

  private static final int COMPRESSION_BUFFER_SIZE = 128 * 1024;
  private static Logger log = Logger.getLogger(VelesManager.class.getName());
  private String _host;
  private int _port;
  private String _workflowId;
  private final Map<String, List<ZMQEndpoint>> _endpoints =
      new TreeMap<String, List<ZMQEndpoint>>();
  private ZMQEndpoint _currentEndpoint;
  private int _counter = 0;
  private int _refresh_interval = 100;

  public int getRefreshInterval() {
    return _refresh_interval;
  }

  public void serRefreshInterval(int value) {
    if (value < 1) {
      throw new IllegalArgumentException();
    }
    _refresh_interval = value;
  }

  public void connect(String host, int port, String workflowId) throws UnknownHostException,
      IOException, NoSlavesExistException {
    synchronized (this) {
      _host = host;
      _port = port;
      _workflowId = workflowId;
      refresh();
    }
  }

  /**
   * Returns response (JSON) from master node.
   *
   * @return Response from master node as continuous byte array.
   * @throws IOException
   */
  private byte[] getResponseFromMaster() throws IOException {
    ArrayList<byte[]> response = new ArrayList<byte[]>();
    int total_length = 0;

    // Send the request to master node
    log.debug(String.format("Communicating with %s:%d...", _host, _port));
    Socket master = new Socket(this._host, this._port);
    try {
      InputStream in = master.getInputStream();
      OutputStream out = master.getOutputStream();
      JSONObject json = new JSONObject();
      json.put("query", "nodes");
      json.put("workflow", _workflowId);
      out.write(JSON.toJSONBytes(json));
      out.write('\r');
      out.write('\n');
      out.flush();
      int length = 0;
      int bufsize = 1024;
      byte[] head = null;
      do {
        head = new byte[bufsize];
        response.add(head);
        length = in.read(head);
        total_length += length;
      } while (head[length - 1] != '\n' || head[length - 2] != '\r');
    } finally {
      master.close();
    }

    // Merge response chunks into a continuous array
    byte[] fullResponse = new byte[total_length];
    int offset = 0;
    for (int i = 0; i < response.size() - 1; i++) {
      byte[] chunk = response.get(i);
      System.arraycopy(chunk, 0, fullResponse, offset, chunk.length);
      offset += chunk.length;
    }
    System.arraycopy(response.get(response.size() - 1), 0, fullResponse, offset, total_length
        - offset);

    return fullResponse;
  }

  /**
   * Parse response (JSON) from master node and update ZeroMQ endpoints.
   *
   * @param response Master node response (JSON).
   * @throws UnknownHostException
   */
  private void updateZmqEndpoints(final byte[] response) throws UnknownHostException {
    // Parse the response - JSON bytes
    JSONObject parsed = (JSONObject) JSON.parse(response);
    _endpoints.clear();
    for (String key : parsed.keySet()) {
      // For each node with ID = key
      JSONObject body = parsed.getJSONObject(key);
      JSONArray data = body.getJSONArray("data");
      JSONObject raw_endpoints = null;
      String hostname = body.getString("host");
      for (Object item : data) {
        if (item == null)
          continue;
        raw_endpoints = (JSONObject) ((JSONObject) item).get("ZmqLoaderEndpoints");
        break;
      }
      // Iterate over endpoint types: tcp, ipc, etc.
      List<ZMQEndpoint> endpoints = new ArrayList<ZMQEndpoint>();
      for (Entry<String, Object> kv : raw_endpoints.entrySet()) {
        String uri = ((JSONArray) kv.getValue()).getString(1);
        // tcp endpoint may contain * instead of IP address
        if (kv.getKey().equals("tcp")) {
          uri = uri.replace("*", hostname);
        }
        endpoints.add(new ZMQEndpoint(hostname, kv.getKey(), uri));
      }
      _endpoints.put(key, endpoints);
    }
  }

  /**
   * Creates a new ZeroMQ DEALER socket, reassigns input and output streams.
   * 
   * openStreams() invalidates getFD() result.
   */
  private void openStreams() {
    if (_socket != null) {
      if (_socket_refs.get(getFD()) == 0) {
        _socket_refs.remove(getFD());
        _socket.close();
      }
    }
    _socket = _context.socket(ZMQ.DEALER);
    _socket_refs.put(getFD(), 0);
    _socket.connect(_currentEndpoint.uri);
    _in = new ZMQInputStream(_socket);
    _out = new ZMQOutputStream(_socket);
  }

  /**
   * Choose nearest ZeroMQ endpoint to current local host using specified EndpointMetrics.
   *
   * @param EndpointMetrics Functor to measure distance between two hosts.
   * @return Nearest ZeroMQ endpoint to the current local host.
   * @throws UnknownHostException
   */
  private void chooseZmqEndpoint(EndpointMetrics EndpointMetrics) throws UnknownHostException {
    java.net.InetAddress localHost = java.net.InetAddress.getLocalHost();
    String localHostName = localHost.getHostName();
    Map<Float, List<ZMQEndpoint>> dist = new TreeMap<Float, List<ZMQEndpoint>>();
    for (Entry<String, List<ZMQEndpoint>> entry : _endpoints.entrySet()) {
      for (final ZMQEndpoint endpoint : entry.getValue()) {
        float distance = EndpointMetrics.distance(endpoint, localHostName);
        if (distance > 1.f) {
          continue;
        }
        if (!dist.containsKey(distance)) {
          dist.put(distance, new ArrayList<ZMQEndpoint>());
        }
        dist.get(distance).add(endpoint);
      }
    }

    List<ZMQEndpoint> list = dist.get(Collections.min(dist.keySet()));
    Random generator = new Random();
    _currentEndpoint = list.get(generator.nextInt(list.size()));
    log.debug(String.format("Selected %s", _currentEndpoint.toString()));
  }

  private void refresh() throws UnknownHostException, IOException, NoSlavesExistException {
    // Get response from master node
    byte[] response = getResponseFromMaster();
    if (response.length == 0) {
      throw new IOException("Empty response from VELES master.");
    }
    // update map of ZeroMQ endpoints
    updateZmqEndpoints(response);
    if (_endpoints.size() == 0) {
      throw new NoSlavesExistException();
    }
    // select the optimal endpoint
    chooseZmqEndpoint(new SameHostMetrics());
    openStreams();
    _counter = 0;
  }

  public String getHost() {
    return _host;
  }

  public int getPort() {
    return _port;
  }

  private final Pickler _pickler = new Pickler();
  private final Unpickler _unpickler = new Unpickler();
  private final ZMQ.Context _context = ZMQ.context(1);
  private ZMQ.Socket _socket;
  private ZMQOutputStream _out;
  private ZMQInputStream _in;
  private HashMap<String, Object> _results = new HashMap<>();
  private TreeMap<String, ZMQInputStream> _pending = new TreeMap<>();
  private TreeMap<Long, Integer> _socket_refs = new TreeMap<>();

  public long getFD() {
    return _socket.getFD();
  }

  public enum Compression {
    None, Gzip, Snappy, Lzma2
  }

  private void pushJob(String id) {
    _socket_refs.put(getFD(), _socket_refs.get(getFD()) + 1);
    _pending.put(id, _in);
  }

  private void popJob(ZMQ.Socket socket, String id) {
    _pending.remove(id);
    int refs = _socket_refs.get(socket.getFD());
    if (refs == 1 && socket != _socket) {
      _socket_refs.remove(socket.getFD());
      socket.close();
    } else {
      _socket_refs.put(socket.getFD(), refs - 1);
    }
  }

  /**
   * Send a new task to be processed by the VELES side, asynchronously. Get the result with yield().
   * The default compression method (Snappy) is used.
   * 
   * @param job The VELES task.
   * @throws PickleException
   * @throws IOException
   * @throws UnsupportedObjectException The specified job object is not pickleable.
   * @throws NoSlavesExistException
   */
  public String submit(Object job) throws IOException, UnsupportedObjectException,
      NoSlavesExistException {
    return submit(job, Compression.Snappy);
  }

  /**
   * Send a new task to be processed by the VELES side, asynchronously. Get the result with yield().
   * 
   * @param job The VELES task.
   * @param compression The compression to use during the submission.
   * @throws PickleException
   * @throws IOException
   * @throws UnsupportedObjectException The specified job object is not pickleable.
   * @throws NoSlavesExistException
   */
  public String submit(Object job, Compression compression) throws IOException,
      UnsupportedObjectException, NoSlavesExistException {
    String id = UUID.randomUUID().toString();
    synchronized (this) {
      if (_counter++ >= _refresh_interval) {
        refresh();
      }
      log.debug(String.format("[%d] submitting a new job of type %s", _counter, job.getClass()
          .toString()));
      OutputStream compressed_out = getCompressedStream(_out, compression, id);
      try {
        _pickler.dump(job, compressed_out);
      } catch (PickleException ex) {
        throw new UnsupportedObjectException();
      }
      compressed_out.close();
      pushJob(id);
    }
    return id;
  }

  /**
   * Block until the result of the task previously sent with submit() is received and return it.
   * 
   * @param id The result identifier (obtained from submit()). If null, any result which has been
   *        already received is returned.
   * @return The result of the VELES processing.
   * @throws IOException
   */
  public Object yield(String id) throws IOException {
    Object res;
    synchronized (this) {
      if (id == null && _results.size() > 0) {
        id = _results.keySet().iterator().next();
      }
      ZMQInputStream in = _pending.get(id);
      while (!_results.containsKey(id)) {
        StringBuilder anotherId = new StringBuilder();
        InputStream uncompressed_in = getUncompressedStream(in, anotherId);
        if (id == null) {
          id = anotherId.toString();
        }
        _results.put(anotherId.toString(), _unpickler.load(uncompressed_in));
        uncompressed_in.close();
        popJob(in.getSocket(), id);
      }
      res = _results.get(id);
      _results.remove(id);
    }
    return res;
  }

  /**
   * Block until any task result is received and return it's identifier. It can be retrieved later
   * via yield(<poll() result>). This method is supposed to work together with epoll()-ing of
   * getFD() in asynchronous frameworks.
   * 
   * @return
   * @throws IOException
   */
  public String poll() throws IOException {
    String id;
    synchronized (this) {
      StringBuilder anotherId = new StringBuilder();
      InputStream uncompressed_in = getUncompressedStream(_in, anotherId);
      id = anotherId.toString();
      _results.put(id, _unpickler.load(uncompressed_in));
      uncompressed_in.close();
    }
    return id;
  }

  /**
   * Execute the VELES side task synchronously, in a blocking manner. The default compression method
   * (Snappy) is used.
   * 
   * @param job The task to send to the remote side.
   * @return The resulting object of the task.
   * @throws PickleException
   * @throws IOException
   * @throws UnsupportedObjectException The specified job object is not pickleable.
   * @throws NoSlavesExistException
   */
  public Object execute(Object job) throws IOException, UnsupportedObjectException,
      NoSlavesExistException {
    return execute(job, Compression.Snappy);
  }

  /**
   * Execute the VELES side task synchronously, in a blocking manner.
   * 
   * @param job The task to send to the remote side.
   * @param compression The data compression algorithm to use.
   * @return The resulting object of the task.
   * @throws PickleException
   * @throws IOException
   * @throws UnsupportedObjectException The specified job object is not pickleable.
   * @throws JobIdMismatchException
   * @throws NoSlavesExistException
   */
  public Object execute(Object job, Compression compression) throws IOException,
      UnsupportedObjectException, NoSlavesExistException {
    String id = submit(job, compression);
    return yield(id);
  }

  private static final byte PICKLE_BEGIN[] = {'v', 'p', 'b'};

  private static class UnflushableBufferedOutputStream extends BufferedOutputStream {
    public UnflushableBufferedOutputStream(OutputStream out, int size) {
      super(out, size);
    }

    @Override
    public void flush() {}

    @Override
    public void close() throws IOException {
      super.flush();
      super.close();
    }
  }

  private static OutputStream getCompressedStream(OutputStream output, Compression compression,
      String id) throws IOException {
    output.write(id.getBytes());
    byte mark[] = new byte[PICKLE_BEGIN.length + 1];
    System.arraycopy(PICKLE_BEGIN, 0, mark, 0, PICKLE_BEGIN.length);
    mark[mark.length - 1] = (byte) compression.ordinal();
    output.write(mark);
    switch (compression) {
      case None:
        return output;
      case Gzip:
        return new GZIPOutputStream(output, COMPRESSION_BUFFER_SIZE);
      case Snappy:
        return new UnflushableBufferedOutputStream(new SnappyFramedOutputStream(output),
            COMPRESSION_BUFFER_SIZE);
      case Lzma2:
        return new UnflushableBufferedOutputStream(new XZOutputStream(output, new LZMA2Options()),
            COMPRESSION_BUFFER_SIZE);
      default:
        throw new UnsupportedOperationException();
    }
  }

  private static InputStream getUncompressedStream(InputStream input, StringBuilder id)
      throws IOException {
    byte[] msgId = new byte[36];
    input.read(msgId);
    id.append(new String(msgId));
    byte[] mark = new byte[PICKLE_BEGIN.length + 1];
    input.read(mark);
    for (int i = 0; i < PICKLE_BEGIN.length; i++) {
      if (mark[i] != PICKLE_BEGIN[i]) {
        throw new IOException("Invalid stream format");
      }
    }
    Compression format = Compression.values()[mark[mark.length - 1]];
    switch (format) {
      case None:
        return input;
      case Gzip:
        return new GZIPInputStream(input);
      case Snappy:
        return new SnappyFramedInputStream(input);
      case Lzma2:
        return new XZInputStream(input);
      default:
        throw new UnsupportedOperationException();
    }
  }
}
