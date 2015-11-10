package com.esgyn.jdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import com.esgyn.util.ConnectionPool;
import com.zaxxer.hikari.HikariDataSource;

public class ExecWork implements Runnable {

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
		synchronized (ExecWork.class) {
			if (conns == null) {
				conns = new ArrayList<Connection>(this.threads);
				for (int i = 0; i < this.threads; i++) {
					conns.add(ConnectionPool.getConn(conf));
				}
			}
		}

	}

	@Override
	public void run() {
		Connection conn = null;
		PreparedStatement ps = null;
		int index = r.nextInt(100) % this.threads;
		conn = conns.get(index);
		synchronized (conn) {
			int[] rs = null;
			try {
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
				count(rows.size());
				if (ps != null) {
					try {
						ps.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				if (rs != null) {
					// for (int i = 0; i < rs.length; i++) {
					// if (rs[i] < 0) {
					// System.out.println("[rs]");
					// for (Object o : rows.get(i)) {
					// System.out.print(o + ",");
					// }
					// System.out.println();
					// }
					// }
					try {
						if (ps != null && ps.getWarnings() != null) {
							ps.getWarnings().printStackTrace();
							Iterator<Throwable> it = ps.getWarnings().iterator();
							while (it.hasNext()) {
								it.next().printStackTrace();
							}
						}
					} catch (SQLException e) {
						System.out.println("Warning:" + e.getMessage());
					}
				}
				// if (conn != null) {
				// try {
				// System.out.println(Thread.currentThread().getName() + " close
				// connection");
				// conn.close();
				// } catch (SQLException e) {
				// e.printStackTrace();
				// }
				// }
			}
		}
	}

	public static List<Connection> getConns() {
		return conns;
	}

	public static void count(int size) {
		synchronized (ExecWork.class) {
			cnt += size;
			System.out.println(cnt +" rows has been inserted!");
		}
	}
}
