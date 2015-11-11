package com.esgyn.jdb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.util.ConnectionPool;
import com.zaxxer.hikari.HikariDataSource;

public class SingleTable implements Callable {
	private static final Logger log = LoggerFactory.getLogger(SingleTable.class);
	private String src;
	private String tgz;
	private ExecutorService tp;
	private HikariDataSource pool;
	private int batchSize;
	private HikariDataSource tgzpool;
	private Properties conf;
	private Map<String, String> colMap = new HashMap<String, String>();
	private int threads;
	private int cacheRows;
	private long cacheSleep;

	public SingleTable(String srcTable, String tgzTable, Properties conf) throws Exception {
		this.src = srcTable;
		this.tgz = tgzTable;

		if (this.src.contains("(")) {
			colMap.clear();
			String[] srctemp = null;
			srctemp = this.src.replaceFirst(".*\\((.*)\\).*", "$1").trim().split("\\s*,\\s*");
			String[] tgztemp = null;
			if (this.tgz.contains("("))
				tgztemp = this.tgz.replaceFirst(".*\\((.*)\\).*", "$1").trim().split("\\s*,\\s*");
			if (srctemp.length != tgztemp.length) {
				throw new Exception("NM columns not match. src cols: " + this.src + " => tgz cols: " + this.tgz);
			}
			for (int i = 0; i < srctemp.length; i++) {
				colMap.put(srctemp[i].trim().toUpperCase(), tgztemp[i].trim().toUpperCase());
			}
			this.src = src.replaceFirst("(.*)\\(.*\\).*", "$1").trim();
			this.tgz = tgz.replaceFirst("(.*)\\(.*\\).*", "$1").trim();
		}
		threads = Integer.parseInt(conf.getProperty("tgz_threads", "5"));
		tp = Executors.newFixedThreadPool(threads);
		pool = ConnectionPool.getSrcPool(conf);
		tgzpool = ConnectionPool.getTgzPool(conf);
		batchSize = Integer.parseInt(conf.getProperty("tgz_batch_size", "1000"));
		cacheRows = Integer.parseInt(conf.getProperty("cache_rows", "100000"));
		cacheSleep = Long.parseLong(conf.getProperty("cache_exceed_sleep_ms", "10000"));
		this.conf = conf;
	}

	@Override
	public Object call() throws Exception {
		int sleep = new Random().nextInt(10);
		System.out.println("loading from " + this.src + " to " + this.tgz + ". " + sleep);
		Connection conn = null;
		int total = 0;
		try {

			String sql = "select * from " + this.src;
			conn = pool.getConnection();
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(sql);
			ResultSetMetaData md = rs.getMetaData();
			String cols = "";
			String qm = "";
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < md.getColumnCount(); i++) {
				if (i > 0) {
					cols += ",";
					qm += ",";
				}
				sb.setLength(0);
				if (colMap.containsKey(md.getColumnName(i + 1).toUpperCase())) {
					sb.append(colMap.get(md.getColumnName(i + 1)));
				} else {
					sb.append(md.getColumnName(i + 1));
				}
				cols += sb.toString();
				qm += "?";
			}
			String insertSql = "upsert using load into " + this.tgz + "(" + cols + ") values(" + qm + ")";
			log.info("insertion sql " + insertSql);
			// start up number of threads waiting for execute
			DataHolder.setDone(false);
			for (int i = 0; i < this.threads; i++) {
				tp.execute(new ExecWork(conf, tgzpool, insertSql));
			}

			List row = null;
			int colcount = md.getColumnCount();
			while (rs.next()) {
				try {
					row = new ArrayList(colcount);
					for (int i = 0; i < md.getColumnCount(); i++) {
						row.add(rs.getObject(i + 1));
					}
					DataHolder.push(row);
					DataHolder.getCountDownLatch().countDown();
					if (DataHolder.size() > this.cacheRows) {
						try {
							log.info("Waiting for consuming data[" + DataHolder.size() + "]!");
							Thread.sleep(this.cacheSleep);
						} catch (Exception e) {
							log.warn(e.getMessage());
						}
					}
				} catch (Exception e) {
					System.out.println("Warning while perform inserting " + e.getMessage());
					e.printStackTrace();
				}
			}
			DataHolder.setDone(true);
			System.out.println(this.src + " " + total + " rows has been selected totally!");

		} catch (Exception e) {
			throw e;
		} finally {
			tp.shutdown();
			tp.awaitTermination(10, TimeUnit.DAYS);
			DataHolder.resetCountDownLatch();
			log.info("thread pool shutdown!");
			if (conn != null) {
				conn.close();
			}
			tgzpool.close();
			pool.close();
		}

		return 0;
	}

}
