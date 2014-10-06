package com.samsung.veles.mastodon.examples.strings;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

import com.samsung.veles.mastodon.NoSlavesExistException;
import com.samsung.veles.mastodon.UnsupportedObjectException;
import com.samsung.veles.mastodon.VelesManager;
import com.samsung.veles.mastodon.VelesManager.Compression;

/**
 * Hello world!
 *
 */
public class App {
  static Logger log = Logger.getLogger(VelesManager.class.getName());

  public static void main(String[] args) throws IOException, UnsupportedObjectException,
      NoSuchAlgorithmException, NumberFormatException, NoSlavesExistException {
    // Calculate the workflow checksum
    String checksum = VelesManager.checksum(args[2]);
    log.info(String.format("Workflow checksum: %s", checksum));

    // By default, the compression used is Snappy. It can be overriden via the cmdline
    Compression compression = Compression.Snappy;
    if (args.length > 3) {
      compression = Compression.valueOf(args[3]);
    }
    log.info(String.format("Compression is %s", compression.toString()));

    // Connect to an instance of VELES
    VelesManager.instance().connect(args[0], Integer.parseInt(args[1]), checksum);

    String job = "";
    while (!job.equals("exit")) {
      job = System.console().readLine();
      Object result = VelesManager.instance().execute(job, compression);
      System.out.println(result);
    }
  }
}
