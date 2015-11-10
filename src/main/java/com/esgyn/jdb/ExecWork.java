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
	private List<List> rows;
	private String sql;
	private Properties conf;
	private int threads;
	private static Random r = new Random();
	private static List<Connection> conns = null;
	private static long cnt = 0;

	public ExecWork(Properties conf, HikariDataSource pool, String sql, List<List> rows) throws Exception {
		this.pool = pool;
		this.rows = rows;
		this.sql = sql;
		this.conf = conf;
		this.threads = Integer.parseInt(conf.getProperty("tgz_threads", "3"));

	}

	@Override
	public void run() {
		Connection conn = null;
		PreparedStatement ps = null;
		int index = r.nextInt(100) % this.threads;
		int[] rs = null;
		try {
			conn = pool.getConnection();
			log.info("Get connection from pool: " + conn);
			// System.out.println(Thread.currentThread().getName() + "
			// loading.....");
			ps = conn.prepareStatement(sql);
			for (List row : rows) {
				for (int i = 0; i < row.size(); i++)
					ps.setObject(i + 1, row.get(i));
				ps.addBatch();
			}
			rs = ps.executeBatch();
		} catch (SQLException e) {
			System.out.println(Thread.currentThread().getName() + " " + e.getMessage());
			e.printStackTrace();
		} finally {
			// count(rows.size());
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
			if (rs != null) {
				try {
					StringBuilder sb = new StringBuilder();
					for (int i = 0; i < rs.length; i++) {
						if (rs[i] < -1) {
							sb.setLength(0);
							for (Object o : rows.get(i)) {
								if (sb.length() > 0) {
									sb.append(",");
								}
								sb.append(o);
							}
							log.warn("Effected rows: " + rs[i] + " | " + sb.toString());
						}
					}
				} catch (Exception e) {
					log.warn("Warning:" + e.getMessage());
				}
			}
		}
	}

}
