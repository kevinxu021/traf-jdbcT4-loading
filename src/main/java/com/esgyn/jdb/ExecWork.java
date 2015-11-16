package com.esgyn.jdb;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecWork implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(ExecWork.class);
	private Properties conf;
	private int threads;
	private int batchSize;
	private String fileName;
	private static long numOfRows = 0;
	private static Random r = new Random();
	private static List<Connection> conns = null;
	private static long cnt = 0;
	private final String newline = System.getProperty("line.separator");
	private NullString ns = new NullString();
	private int separator;

	public ExecWork(Properties conf) throws Exception {
		this.conf = conf;
		this.threads = Integer.parseInt(conf.getProperty("tgz_threads", "3"));
		this.batchSize = Integer.parseInt(conf.getProperty("tgz_batch_size", "1000"));
		this.fileName = conf.getProperty("tgz_file_name", "temp");
		this.separator = Integer.parseInt(conf.getProperty("tgz_row_separator", "7"));

	}

	@Override
	public void run() {
		try {
			DataHolder.getCountDownLatch().await();
		} catch (InterruptedException e1) {
			log.error(e1.getMessage(), e1);
		}
		int total = 0;
		FileOutputStream out = null;
		try {
			this.fileName = this.fileName + Thread.currentThread().getId() + ".json";
			out = new FileOutputStream(this.fileName, false);
			log.info("Write data into: " + this.fileName);
			List row = null;
			int n = 0;
			String tmp = null;
			while (true) {
				row = DataHolder.pop();
				if (row != null) {
					for (int i = 0; i < row.size(); i++) {
						if (i > 0) {
							out.write(this.separator);
						}
						tmp = row.get(i) + "";
						out.write((row.get(i) == null ? "" : tmp).getBytes());
						if (tmp.contains((char) this.separator + "")) {
							log.error("NM! You have perticular character ACII(" + this.separator + ").");
							System.exit(0);
						}
					}
					out.write(this.newline.getBytes());
					n++;
				} else if (DataHolder.isDone()) {
					break;
				} else {
					try {
						log.info("Waiting for more data!");
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						log.warn(e.getMessage());
					}
				}
				if (n >= this.batchSize) {
					out.flush();
					total += n;
					n = 0;
				}
			}

			if (n > 0) {
				total += n;
				n = 0;
			}

		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			if (out != null)
				try {
					out.close();
				} catch (IOException e) {
					log.warn(e.getMessage());
				}
			count(total);
		}
	}

	private void count(int total) {
		synchronized (ExecWork.class) {
			numOfRows += total;
			log.info(numOfRows + " rows has been inserted! ");
		}
	}

}
