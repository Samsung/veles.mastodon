package com.samsung.veles.mastodon.examples.strings;

import java.io.IOException;

import com.samsung.veles.mastodon.UnsupportedObjectException;
import com.samsung.veles.mastodon.VelesManager;

/**
 * Hello world!
 *
 */
public class App {
  public static void main(String[] args) throws IOException, UnsupportedObjectException {
    VelesManager.instance().connect(args[0], Integer.parseInt(args[1]), args[2]);
    String job = "";
    while (job.equals("exit")) {
      job = System.console().readLine();
      VelesManager.instance().execute(job);
    }
  }
}
