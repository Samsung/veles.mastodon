package com.samsung.veles.mastodon.examples.strings;

import java.io.IOException;

import net.razorvine.pickle.PickleException;

import com.samsung.veles.mastodon.VelesManager;

/**
 * Hello world!
 *
 */
public class App {
  public static void main(String[] args) throws PickleException, IOException {
    VelesManager.instance().connect(args[1], Integer.parseInt(args[2]), args[3]);
    String job = "";
    while (job.equals("exit")) {
      job = System.console().readLine();
      VelesManager.instance().execute(job);
    }
  }
}
