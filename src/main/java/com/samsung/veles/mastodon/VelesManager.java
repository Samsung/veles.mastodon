package com.samsung.veles.mastodon;

import java.io.BufferedInputStream;
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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;
import net.razorvine.pickle.Unpickler;

import org.apache.log4j.Logger;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;
import org.zeromq.ZMQ;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Connects to Veles workflow's master and submits jobs.
 *
 */
public class VelesManager {
  private static final byte[] IDENTITY = {'M', 'a', 's', 't', 'o', 'd', 'o', 'n'};
  static Logger log = Logger.getLogger(VelesManager.class.getName());

  public enum Compression {
    None, Gzip, Snappy, Lzma2
  }

  private static volatile VelesManager _instance = null;

  public static VelesManager instance() {
    if (_instance == null) {
      synchronized (VelesManager.class) {
        if (_instance == null) {
          _instance = new VelesManager();
        }
      }
    }
    return _instance;
  }

  public static class ZmqEndpoint {
    public ZmqEndpoint(String host, String type, String uri) {
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
      if (!(other instanceof ZmqEndpoint))
        return false;

      ZmqEndpoint endpoint = (ZmqEndpoint) other;
      if (this.host.equals(endpoint.host) && this.uri.equals(endpoint.uri)
          && this.type.equals(endpoint.type)) {
        return true;
      } else {
        return false;
      }
    }

    public String host;
    public String uri;
    public String type;
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

  private String _host;
  private int _port;
  private String _workflowId;
  private final Map<String, List<ZmqEndpoint>> _endpoints =
      new TreeMap<String, List<ZmqEndpoint>>();
  private ZmqEndpoint _currentEndpoint;

  public void connect(String host, int port, String workflowId) throws UnknownHostException,
      IOException {
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
    Socket master = new Socket(this._host, this._port);
    try {
      InputStream in = master.getInputStream();
      OutputStream out = master.getOutputStream();
      JSONObject json = new JSONObject();
      json.put("query", "nodes");
      json.put("workflow", _workflowId);
      out.write(JSON.toJSONBytes(json));
      int length = 0;
      int bufsize = 1024;
      byte[] head = null;
      do {
        head = new byte[bufsize];
        response.add(head);
        length = in.read(head);
        total_length += length;
      } while (length == bufsize && head[length - 1] != '\n');
    } finally {
      master.close();
    }

    // Merge response chunks into a continuous array
    byte[] fullResponse = null;
    if (response.size() > 1) {
      fullResponse = new byte[total_length];
      int offset = 0;
      for (int i = 0; i < response.size(); i++) {
        byte[] chunk = response.get(i);
        System.arraycopy(chunk, 0, fullResponse, offset, chunk.length);
        offset += chunk.length;
      }
    } else {
      fullResponse = response.get(0);
    }

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
      List<ZmqEndpoint> endpoints = new ArrayList<ZmqEndpoint>();
      for (Entry<String, Object> kv : raw_endpoints.entrySet()) {
        String uri = ((JSONArray) kv.getValue()).getString(1);
        // tcp endpoint may contain * instead of IP address
        if (kv.getKey().equals("tcp")) {
          uri = uri.replace("*", hostname);
        }
        endpoints.add(new ZmqEndpoint(hostname, kv.getKey(), uri));
      }
      _endpoints.put(key, endpoints);
    }
  }

  /**
   * Creates a new ZeroMQ DEALER socket, reassigns input and output streams.
   *
   */
  private void openStreams() {
    ZMQ.Socket socket = _context.socket(ZMQ.DEALER);
    socket.setIdentity("Mastodon".getBytes());
    socket.connect(_currentEndpoint.uri);
    _in = new ZMQInputStream(socket);
    _out = new ZMQOutputStream(socket, IDENTITY);
  }

  public interface Metrics {
    public float distance(String host1, String host2);
  }

  public class SimpleMetrics implements Metrics {
    @Override
    public float distance(String host1, String host2) {
      return host1.equals(host2) ? 0 : 1;
    }
  }

  /**
   * Choose nearest ZeroMQ endpoint to current local host using specified metrics.
   *
   * @param metrics Functor to measure distance between two hosts.
   * @return Nearest ZeroMQ endpoint to the current local host.
   * @throws UnknownHostException
   */
  private void chooseZmqEndpoint(Metrics metrics) throws UnknownHostException {
    java.net.InetAddress localHost = java.net.InetAddress.getLocalHost();
    String curHostName = localHost.getHostName();
    Map<Float, List<ZmqEndpoint>> dist = new TreeMap<Float, List<ZmqEndpoint>>();
    for (Entry<String, List<ZmqEndpoint>> entry : _endpoints.entrySet()) {
      for (final ZmqEndpoint endpoint : entry.getValue()) {
        dist.put(metrics.distance(endpoint.host, curHostName), entry.getValue());
      }
    }

    List<ZmqEndpoint> list = dist.get(Collections.min(dist.keySet()));
    Random generator = new Random();
    _currentEndpoint = list.get(generator.nextInt(list.size()));
  }

  private void refresh() throws UnknownHostException, IOException {
    // Get response from master node
    byte[] response = getResponseFromMaster();
    // update map of ZeroMQ endpoints
    updateZmqEndpoints(response);
    // select the optimal endpoint
    chooseZmqEndpoint(new SimpleMetrics());
    openStreams();
  }

  public String getHost() {
    return _host;
  }

  public int getPort() {
    return _port;
  }

  private Pickler _pickler;
  private Unpickler _unpickler;
  private final ZMQ.Context _context = ZMQ.context(1);
  private ZMQOutputStream _out;
  private ZMQInputStream _in;

  public Object execute(Object job) throws PickleException, IOException {
    return execute(job, Compression.Snappy);
  }

  /**
   * Execute the VELES model synchronously, in a blocking manner.
   * 
   * @param job The task to send to the remote side.
   * @param compression The data compression algorithm to use.
   * @return The resulting object of the task.
   * @throws PickleException
   * @throws IOException
   */
  public Object execute(Object job, Compression compression) throws PickleException, IOException {
    Object res = null;
    synchronized (this) {
      _out.start(); // send the identity
      OutputStream compressed_out = getCompressedStream(_out, compression);
      _pickler.dump(job, compressed_out);
      closeCompressedStream(compressed_out);
      _out.finish(); // mark the end of the current pickle
      res = _unpickler.load(getUncompressedStream(_in));
    }
    return res;
  }

  private static final byte PICKLE_BEGIN[] = {'v', 'p', 'b'};

  private static OutputStream getCompressedStream(OutputStream output, Compression compression)
      throws IOException {
    byte mark[] = new byte[PICKLE_BEGIN.length + 1];
    System.arraycopy(PICKLE_BEGIN, 0, mark, 0, PICKLE_BEGIN.length);
    mark[mark.length - 1] = (byte) compression.ordinal();
    output.write(mark);
    switch (compression) {
      case None:
        return output;
      case Gzip:
        return new GZIPOutputStream(output);
      case Snappy:
        return new SnappyOutputStream(output);
      case Lzma2:
        return new XZOutputStream(output, new LZMA2Options());
      default:
        throw new UnsupportedOperationException();
    }
  }

  private static void closeCompressedStream(OutputStream out) throws IOException {
    if (out instanceof GZIPOutputStream) {
      ((GZIPOutputStream) out).finish();
    }
    out.flush();
  }

  private static InputStream getUncompressedStream(InputStream input) throws IOException {
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
        return new SnappyInputStream(input);
      case Lzma2:
        return new XZInputStream(input);
      default:
        throw new UnsupportedOperationException();
    }
  }
}
