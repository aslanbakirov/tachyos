package com.apache.mesos.tachyos;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @author Aslan Bakirov Main class of Tachyon on Mesos framework. Framework scheduler is
 *         initialized and started in main method.
 * 
 */
public class Main {

  public static final Log log = LogFactory.getLog(Main.class);

  public static void main(String[] args) throws Exception {

    Thread scheduler = new Thread(new Scheduler());
    scheduler.start();
  }

}
