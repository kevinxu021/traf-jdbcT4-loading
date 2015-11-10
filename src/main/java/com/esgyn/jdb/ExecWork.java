package com.esgyn.jdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

public class ExecWork implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(ExecWork.class);
	private HikariDataSource pool;
	private String sql;
	private Properties conf;
	private int threads;
	private int batchSize;
	private int numOfRows;
	private static Random r = new Random();
	private static List<Connection> conns = null;
	private static long cnt = 0;

	public ExecWork(Properties conf, HikariDataSource pool, String sql) throws Exception {
		this.pool = pool;
		this.sql = sql;
		this.conf = conf;
		this.threads = Integer.parseInt(conf.getProperty("tgz_threads", "3"));
		this.batchSize = Integer.parseInt(conf.getProperty("tgz_batch_size", "1000"));

	}

	@Override
	public void run() {
		try {
			DataHolder.getCountDownLatch().await();
		} catch (InterruptedException e1) {
			log.error(e1.getMessage(), e1);
		}
		Connection conn = null;
		PreparedStatement ps = null;
		int[] rs = null;
		int total = 0;
		try {
			conn = pool.getConnection();
			log.info("Get connection from pool: " + conn);
			ps = conn.prepareStatement(sql);
			List row = null;
			int n = 0;
			while (true) {
				row = DataHolder.pop();
				if (row != null) {
					for (int i = 0; i < row.size(); i++) {
						ps.setObject(i + 1, row.get(i));
					}
					ps.addBatch();
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
					ps.executeBatch();
					n = 0;
				}
			}

			if (n > 0) {
				ps.executeBatch();
				n = 0;
			}

		} catch (SQLException e) {
			log.error(e.getMessage(), e);
		} finally {
			count(total);
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					log.warn(e.getMessage(), e);
				}
			}
			if (conn != null) {
				try {
					log.info("return back to pool");
					conn.close();
				} catch (SQLException e) {
					log.warn(e.getMessage(), e);
				}
			}
		}
	}

	private void count(int total) {
		synchronized (ExecWork.class) {
			this.numOfRows += total;
			log.info(this.numOfRows + " rows has been inserted!");
		}
	}

}
