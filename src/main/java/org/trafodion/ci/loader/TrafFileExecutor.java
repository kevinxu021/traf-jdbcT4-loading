package org.trafodion.ci.loader;

import java.io.File;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.PrefixFileFilter;

public class TrafFileExecutor implements Runnable {

	private File file;
	private Properties config;
	private TrafLoader loader;
	private boolean withHead;
	private String delimiter;
	private TrafTargetTable tableDesc;
	private File errRowFile;
	private String user;
	private String pwd;
	private String url;
	private String sql;
	private int batchSize;
	private boolean saveErrRows;
	private boolean stopOnError;
	private File tmpRowFile;

	public TrafFileExecutor(TrafLoader loader, File file, Properties config, TrafTargetTable targetTable) {
		this.file = file;
		this.errRowFile = new File(file.getAbsolutePath() + ".err");
		this.tmpRowFile = new File(file.getAbsolutePath() + ".tmp");
		this.config = config;
		this.loader = loader;
		this.withHead = Boolean.parseBoolean(config.getProperty("with_head", "false"));
		this.tableDesc = targetTable;
		String targetTableName = config.getProperty("target_table", "abc");
		String source_path = config.getProperty("source_path", "d:\\test");
		this.delimiter = config.getProperty("delimiter", "~");
		String skiped = config.getProperty("skipped_rows_posix", ".rows");
		this.batchSize = Integer.parseInt(config.getProperty("batch_size", "5000"));
		int max_thread_size = Integer.parseInt(config.getProperty("max_thread_size", "10"));
		this.user = config.getProperty("user", "zz");
		this.pwd = config.getProperty("pwd", "zz");
		this.url = config.getProperty("url", "jdbc:t4jdbc://10.10.10.136:23400/:maxPoolSize=" + max_thread_size);
		StringBuilder tk = new StringBuilder();
		for (int i = 0; i < targetTable.getColDesc().size(); i++) {
			tk.append("?");
			if (i < targetTable.getColDesc().size() - 1) {
				tk.append(",");
			}
		}
		this.sql = "upsert using load into " + targetTableName + " values(" + tk + ")";
		this.saveErrRows = Boolean.parseBoolean(config.getProperty("save_error_rows", "false"));
		this.stopOnError = Boolean.parseBoolean(config.getProperty("stop_on_error", "true"));

	}

	@Override
	public void run() {
		int rowcount = 0;
		int batchCnt = 0;
		long before = System.currentTimeMillis();
		if (this.file.canRead()) {
			LineIterator it = null;
			Connection conn = null;
			PreparedStatement ps = null;
			try {
				conn = DriverManager.getConnection(url, user, pwd);
				ps = conn.prepareStatement(sql);
				it = FileUtils.lineIterator(file);
				String line = null;
				if (withHead) {
					if (it.hasNext()) {
						line = it.next();
						// deal with the head and create the table if not exists
					}
				}
				// remove error file
				if (this.errRowFile.exists()) {
					FileUtils.forceDelete(this.errRowFile);
				}
				// remove temp files
				Iterator<File> errlistIt = FileUtils.listFiles(this.file.getParentFile(),
						new PrefixFileFilter(this.tmpRowFile.getName()), DirectoryFileFilter.DIRECTORY).iterator();
				if (errlistIt.hasNext()) {
					FileUtils.forceDelete(errlistIt.next());
				}
				String[] values = null;
				int index = 1;
				outter: while (it.hasNext()) {
					line = it.next();
					values = line.split(delimiter);
					if (values.length < this.tableDesc.getColDesc().size()) {
						FileUtils.writeStringToFile(this.errRowFile, line + "\n", true);
						continue;
					}
					for (int i = 0; i < values.length; i++) {
						index = i + 1;
						try {
							ps.setObject(index, getValue(values[i], this.tableDesc.getColDesc().get(i)));
						} catch (Exception e) {
							e.printStackTrace();
							saveErrRows(line);
							if (this.stopOnError)
								break outter;
							ps.setNull(index, (int) (this.tableDesc.getColDesc().get(i)[1]));
						}
					}
					ps.addBatch();
					++batchCnt;
					if (batchCnt >= this.batchSize) {
						try {
							ps.executeBatch();
						} catch (BatchUpdateException e) {
							Iterator<Throwable> eit = e.iterator();
							while (eit.hasNext()) {
								eit.next().printStackTrace();
							}
							// saveErrRows(line);
							System.out.println("Error row index:" + (rowcount + 1) + " - " + (rowcount + batchCnt));
							if (this.stopOnError)
								break outter;
						}
						rowcount += batchCnt;
						batchCnt = 0;
					}
				}

				if (batchCnt > 0) {
					try {
						ps.executeBatch();
					} catch (BatchUpdateException e) {
						Iterator<Throwable> eit = e.iterator();
						while (eit.hasNext()) {
							eit.next().printStackTrace();
						}
						saveErrRows(line);
						System.out.println("Error row index:" + (rowcount + 1) + " - the last row");
						if (this.stopOnError)
							throw e;

					}
					rowcount += batchCnt;
					batchCnt = 0;
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (it != null) {
					it.close();
				}
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
						e.printStackTrace();
					}
				}
			}
		}
		long end = System.currentTimeMillis();
		try {
			this.loader.countDown(Thread.currentThread().getName(), this.file, rowcount, end - before);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void saveErrRows(String line) {
		// TODO Auto-generated method stub

	}

	private Object getValue(String value, Object colDesc[]) throws Exception {
		int type = (int) colDesc[1];
		switch (type) {
		case Types.CHAR:
		case Types.VARCHAR:
			return value;
		case Types.TIMESTAMP:
			return Timestamp.valueOf(value);
		case Types.DATE:
			return Date.valueOf(value);
		case Types.INTEGER:
			return Integer.valueOf(value);
		case Types.BIGINT:
			return Long.valueOf(value);
		default:
			throw new Exception("Not supported type:" + type + ", column name:" + colDesc[0] + ", value:" + value);

		}
	}

}
