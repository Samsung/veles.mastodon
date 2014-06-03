package com.samsung.veles.mastodon;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;
import net.razorvine.pickle.Unpickler;

/**
 * Connects to Veles workflow's master and submits jobs.
 * 
 */
public class VelesManager {
  public enum Compression {
    None,
    Gzip,
    Snappy,
    Lzma2
  }
  
  private static volatile VelesManager _instance;
  
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
  
  private String _host;
  private int _port;
  private String _workflowId;
  private Map<String, Map<String, String>> _endpoints =
      new TreeMap<String, Map<String, String>>();
  
  public void connect(String host, int port, String workflowId)
      throws UnknownHostException, IOException {
    synchronized (this) {
      _host = host;
      _port = port;
      _workflowId = workflowId;
      refresh();
    }
  }
  
  private void refresh() throws UnknownHostException, IOException {
    ArrayList<byte[]> response = new ArrayList<byte[]>();
    int total_length = 0;
    // Send the request to master node
    Socket master = new Socket(this._host, this._port);
    try {
      InputStream in = master.getInputStream();
      OutputStream out = master.getOutputStream();
      JSONObject json = new JSONObject();
      json.put("query", "nodes");
      out.write(JSON.toJSONBytes(json));      
      int length = 0;      
      int bufsize = 1024;
      byte[] head = null;
      do {
        head = new byte[bufsize];
        response.add(head);
        length = in.read(head);
        total_length += length;
      }
      while (length == bufsize && head[length - 1] != '\n');
    }
    finally {
      master.close();
    }
    // Merge response chunks into a continuous array
    byte[] all = null;
    if (response.size() > 1) {
      all = new byte[total_length];
      int offset = 0;
      for (int i = 0; i < response.size(); i++) {
        byte[] chunk = response.get(i);
        System.arraycopy(chunk, 0, all, offset, chunk.length);
        offset += chunk.length;
      }
    } else {
      all = response.get(0);
    }
    // Parse the response - JSON bytes
    JSONObject parsed = (JSONObject)JSON.parse(all);
    _endpoints.clear();
    for (String key : parsed.keySet()) {
      // For each node with ID = key
      Map<String, String> endpoints = new TreeMap<String, String>();
      JSONObject body = parsed.getJSONObject(key);
      JSONArray data = body.getJSONArray("data");
      JSONObject raw_endpoints = null;
      String hostname = body.getString("host");
      for (Object item : data) {
        if (item == null) {
          continue;
        }
        raw_endpoints = (JSONObject)((JSONObject)item).get(
            "ZmqLoaderEndpoints");
        break;
      }
      // Now we have JSONObject with endpoints, raw_endpoints.
      // Iterate over endpoint types: tcp, ipc, etc.
      for (Entry<String, Object> kv : raw_endpoints.entrySet()) {
        String value = ((JSONArray)kv.getValue()).getString(1);
        // tcp endpoint may contain * instead of IP address
        if (kv.getKey().equals("tcp")) {
          value = value.replace("*", hostname);
        }
        endpoints.put(kv.getKey(), value);
      }
      _endpoints.put(key, endpoints);
    }
    // TODO(v.markovtsev): select the optimal endpoint
    // TODO(v.markovtsev): implement creating _out and _in
  }
  
  public String getHost() {
    return _host;
  }
  
  public int getPort() {
    return _port;
  }
  
  public String getWorkflowId() {
    return _workflowId;
  }
  
  private Pickler _pickler;
  private Unpickler _unpickler;
  private ZeroMQOutputStream _out;
  private ZeroMQInputStream _in;
  
  public Object execute(Object job)
      throws PickleException, IOException {
    return execute(job, Compression.Snappy);
  }
  
  public Object execute(Object job, Compression compression)
      throws PickleException, IOException {
    Object res = null;
    synchronized (this) {
      _pickler.dump(job, getCompressedStream(_out, compression));
      _out.finish();
      res = _unpickler.load(getUncompressedStream(_in));
    }
    return res;
  }
  
  private static final byte PICKLE_BEGIN[] = {'v', 'p', 'b'};
  private static final byte PICKLE_END[] = {'v', 'p', 'e'};

  private static OutputStream getCompressedStream(OutputStream output,
      Compression compression) throws IOException {
    byte mark[] = new byte[PICKLE_BEGIN.length + 1];
    System.arraycopy(PICKLE_BEGIN, 0, mark, 0, PICKLE_BEGIN.length);
    mark[mark.length - 1] = (byte)compression.ordinal();
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
  
  private static InputStream getUncompressedStream(InputStream input)
      throws IOException {
    byte[] mark = new byte[PICKLE_END.length + 1];
    input.read(mark);
    for (int i = 0; i < PICKLE_END.length; i++) {
      if (mark[i] != PICKLE_END[i]) {
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
