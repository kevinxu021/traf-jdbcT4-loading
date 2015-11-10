package com.esgyn.util;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ConnectionPool implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(ConnectionPool.class);
	private static HikariDataSource tgzpool;
	private static HikariDataSource srcpool;

	public synchronized static HikariDataSource getSrcPool(Properties options) throws Exception {
		if (srcpool == null) {
			if (options == null) {
				log.error("Please set configuration first!");
				System.exit(0);
			}

			HikariConfig config = new HikariConfig();
			config.setMaximumPoolSize(300);
			config.setIdleTimeout(60000);
			config.setDriverClassName(options.getProperty("src_driver", "org.trafodion.jdbc.t4.T4Driver"));
			config.setJdbcUrl(options.getProperty("src_url"));
			config.setUsername(options.getProperty("src_user"));
			config.setPassword(options.getProperty("src_pwd"));
			config.addDataSourceProperty("cachePrepStmts", "true");
			config.addDataSourceProperty("prepStmtCacheSize", "250");
			config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
			config.setConnectionTimeout(30000);
			srcpool = new HikariDataSource(config);
		}
		return srcpool;
	}

	public synchronized static HikariDataSource getTgzPool(Properties options) throws Exception {
		if (tgzpool == null) {
			if (options == null) {
				log.error("Please set configuration first!");
				System.exit(0);
			}
			HikariConfig config = new HikariConfig();
			config.setMaximumPoolSize(Integer.parseInt(options.getProperty("tgz_threads", "10")));
			config.setConnectionTestQuery("values(0)");
			config.setIdleTimeout(60000);
			config.setDriverClassName(options.getProperty("tgz_driver", "org.trafodion.jdbc.t4.T4Driver"));
			config.setJdbcUrl(options.getProperty("tgz_url"));
			config.setUsername(options.getProperty("tgz_user"));
			config.setPassword(options.getProperty("tgz_pwd"));
			config.addDataSourceProperty("cachePrepStmts", "true");
			config.addDataSourceProperty("prepStmtCacheSize", "250");
			config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
			config.setConnectionTimeout(30000);
			tgzpool = new HikariDataSource(config);
		}
		return tgzpool;
	}

	public synchronized static Connection getConn(Properties options) throws Exception {
		Class.forName(options.getProperty("tgz_driver", "org.trafodion.jdbc.t4.T4Driver"));
		return DriverManager.getConnection(options.getProperty("tgz_url"), options.getProperty("tgz_user"),
				options.getProperty("tgz_pwd"));
	}

	public static void main(String[] args) throws Exception {
		String content = "helloworld" + new Random().nextInt(100);
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			System.out.println("before get connection");

			ps = conn.prepareStatement("insert /*+direct*/ into mfg_pms.sai_test_msmq values(current_timestamp,?)");
			ps.setObject(1, content);
			// String sql =
			// "insert into dev.sai_test_msmq values (current_timestamp, '" +
			// content + "')";
			// ps = conn.prepareStatement(sql);
			ps.execute();
			System.out.println("success");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}

}
