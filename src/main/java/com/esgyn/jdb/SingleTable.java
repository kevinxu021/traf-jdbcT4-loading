package com.esgyn.jdb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.esgyn.util.ConnectionPool;
import com.zaxxer.hikari.HikariDataSource;

public class SingleTable implements Callable {

	private String src;
	private String tgz;
	private ExecutorService tp;
	private HikariDataSource pool;
	private int batchSize;
	private HikariDataSource tgzpool;
	private Properties conf;

	public SingleTable(String srcTable, String tgzTable, Properties conf) throws Exception {
		this.src = srcTable;
		this.tgz = tgzTable;
		tp = Executors.newFixedThreadPool(Integer.parseInt(conf.getProperty("tgz_threads", "5")));
		pool = ConnectionPool.getSrcPool(conf);
		tgzpool = ConnectionPool.getTgzPool(conf);
		batchSize = Integer.parseInt(conf.getProperty("tgz_batch_size", "1000"));
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
			for (int i = 0; i < md.getColumnCount(); i++) {
				if (i > 0) {
					cols += ",";
					qm += ",";
				}
				cols += md.getColumnName(i + 1);
				qm += "?";
			}
			String insertSql = "upsert using load into " + this.tgz + "(" + cols + ") values(" + qm + ")";
			System.out.println("insertion sql " + insertSql);

			List<List> rows = null;
			int n = 0;
			List row = null;
			while (rs.next()) {
				try {
					if (n == 0) {
						rows = new ArrayList<List>(this.batchSize);
					}
					row = new ArrayList();
					for (int i = 0; i < md.getColumnCount(); i++) {
						row.add(rs.getObject(i + 1));
					}
					rows.add(row);
					if (n++ >= this.batchSize) {
						total += this.batchSize;
						n = 0;
						tp.execute(new ExecWork(conf, tgzpool, insertSql, rows));
						if(total%100000 ==0){
							Thread.sleep(5000);
						}
					}
				} catch (Exception e) {
					System.out.println("Warning while perform inserting " + e.getMessage());
					e.printStackTrace();
				}
			}
			if (n > 0) {
				total += n;
				n = 0;
				tp.execute(new ExecWork(this.conf, tgzpool, insertSql, rows));
			}
			System.out.println(this.src + " " + total + " rows has been selected totally!");

		} catch (Exception e) {
			throw e;
		} finally {
			tp.shutdown();
			tp.awaitTermination(100, TimeUnit.DAYS);
			System.out.println("thread pool shutdown!");
			for (Connection con : ExecWork.getConns()) {
				if (con != null) {
					con.close();
				}
			}
			if (conn != null) {
				conn.close();
			}
			tgzpool.close();
			pool.close();
		}

		return 0;
	}

}
