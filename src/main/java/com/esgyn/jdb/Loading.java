package com.esgyn.jdb;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Loading {
	private static Logger log = LoggerFactory.getLogger(Loading.class);

	public static void main(String[] args) throws Exception {
		String path = System.getProperty("configFile");
		if (path == null) {
			log.error("Config file does not exist!");
			System.out.println("Config file does not exist!");
			System.exit(0);
		}
		Properties conf = new Properties();
		conf.load(new FileInputStream(path));
		String srcTables = conf.getProperty("src_tables");
		String tgzTables = conf.getProperty("tgz_tables");
		if (srcTables == null) {
			System.out.println("src_tables does not exist!");
			System.exit(0);
		}
		if (tgzTables == null) {
			tgzTables = srcTables;
		}
		String[] srcs = srcTables.split("\\s*,\\s*");
		String[] tgzs = tgzTables.split("\\s*,\\s*");

		ExecutorService tabs = Executors.newSingleThreadExecutor();
		List<Future> futures = new ArrayList<Future>();
		for (int i = 0; i < srcs.length; i++) {
			futures.add(tabs.submit(new SingleTable(srcs[i], tgzs[i], conf)));
		}
		for (int i = 0; i < futures.size(); i++) {
			try {
				Object rs = futures.get(i).get();
				if (!rs.equals(0))
					log.info(srcs[i] + " -> " + tgzs[i] + " done with error! " + (i + 1) + "/" + futures.size());
				else
					log.info(srcs[i] + " -> " + tgzs[i] + " done! " + (i + 1) + "/" + futures.size());
			} catch (Exception e) {
				System.out.println("Error while loading table from " + srcs[i] + " to " + tgzs[i] + ".");
				e.printStackTrace();
			}
		}

		tabs.shutdown();

	}

}
