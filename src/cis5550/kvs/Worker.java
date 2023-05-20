package cis5550.kvs;

import static cis5550.webserver.Server.*;
import cis5550.tools.*;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public class Worker extends cis5550.generic.Worker {
	private static final Logger logger = Logger.getLogger(Worker.class);

	private static Map<String, Map<String, Row>> tables = new ConcurrentHashMap<>();
	private static Map<String, RandomAccessFile> tableFileLoc = new ConcurrentHashMap<>();
	private static Map<String, Map<String, persistRow>> persistTables = new ConcurrentHashMap<>();
	private static int numPageShow = 10;
	private static boolean hasReceiveRequest = false;

	public static class persistRow {
		String key;
		long offset = -1;

		persistRow(String key) {
			this.key = key;
		}

		persistRow(String key, long offset) {
			this.key = key;
			this.offset = offset;
		}

		public void setOffset(long offset) {
			this.offset = offset;
		}

		public String getKey() {
			return this.key;
		}

		public long getOffset() {
			return this.offset;
		}

		public synchronized persistRow clone() {
			return new persistRow(this.getKey(), this.getOffset());
		}
	}

	public static void main(String[] args) {
		if (args.length != 3) {
			logger.error("kvs.Worker: Wrong number of arguments.");
			System.exit(0);
		}
		int workerPort = Integer.valueOf(args[0]);
		String directory = args[1];
		String remoteHost = args[2];
		port(workerPort);

		String workerID = "";
		try {
			File widDir = new File(directory);
			if (!widDir.exists()) {
				widDir.mkdirs();
			}
			File widFile = new File(String.join(File.separator, directory, "id"));
			if (widFile.exists()) {
				BufferedReader reader = new BufferedReader(new FileReader(widFile));
				workerID = reader.readLine();
				reader.close();
			} else {
				Random rand = new Random();
				for (int i = 0; i < 5; i++) {
					workerID += "abcdefghijklmnopqrstuvwxyz".charAt(rand.nextInt(25));
				}
				PrintWriter writer = new PrintWriter(String.join(File.separator, directory, "id"));
				writer.write(workerID);
				writer.close();
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		startPingThread(remoteHost, workerID, workerPort + "");

//		startGarbageCollector(directory);

		// do recovery
		File[] files = (new File(directory)).listFiles();
		try {
			if (files != null && files.length > 0) {
				for (File f : files) {
					String fName = f.getName();
					if (!fName.endsWith(".table"))
						continue;
					Map<String, persistRow> pTable = new ConcurrentHashMap<>();
					RandomAccessFile raf = new RandomAccessFile(f, "rw");
					synchronized (raf) {
						for (long offset = 0; offset < raf.length();) {
							Row row;
							raf.seek(offset);
							row = Row.readFrom(raf);
							pTable.put(row.key, new persistRow(row.key, offset));
							offset += (row.toByteArray().length + 1);
						}
						String tableName = fName.substring(0, fName.length() - 6);
						persistTables.put(tableName, pTable);
						tableFileLoc.put(tableName, raf);
					}
					logger.info("table " + fName + " recovery done.");
					System.out.println("table " + fName + " recovery done.");
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		put("/persist/:T", (req, rsp) -> {
			hasReceiveRequest = true;
			String tableName = req.params("T");
			int status = 403;
			String content = "Forbidden";
			if (!persistTables.containsKey(tableName)) {
				RandomAccessFile raf = new RandomAccessFile(directory + File.separator + tableName + ".table", "rw");
				synchronized (raf) {
					persistTables.put(tableName, new ConcurrentHashMap<>());
					tableFileLoc.put(tableName, raf);
				}
				status = 200;
				content = "OK";
			}
			rsp.status(status, content);
			rsp.type("text/html");
			rsp.header("Content-Length", String.valueOf(content.length()));
			rsp.bodyAsBytes(content.getBytes());
			return null;
		});

		put("/data/:T/:R/:C", (req, rsp) -> {
			hasReceiveRequest = true;
			String tableName = req.params("T");
			String rowName = req.params("R");
			String colName = req.params("C");
			String ifColumn = req.queryParams("ifcolumn");
			String equals = req.queryParams("equals");
			String pTablePath = directory + File.separator + tableName + ".table";
			boolean isPut = putRow(tableName, rowName, colName, req.bodyAsBytes(), ifColumn, equals, pTablePath);
			String content = isPut ? "OK" : "FAIL";
			rsp.status(200, content);
			rsp.type("text/html");
			rsp.header("Content-Length", String.valueOf(content.length()));
			rsp.bodyAsBytes(content.getBytes());
			return null;
		});

		put("/data2/:T", (req, rsp) -> {
			hasReceiveRequest = true;
			String tableName = req.params("T");
			String ifColumn = req.queryParams("ifcolumn");
			String equals = req.queryParams("equals");
			String pTablePath = directory + File.separator + tableName + ".table";
			ByteArrayInputStream bis = new ByteArrayInputStream(req.bodyAsBytes());
			Row row;
			boolean isPut = true;
			int count = 0;
			while ((row = Row.readFrom(bis)) != null) {
				count++;
				for (String c : row.columns()) {
					isPut = putRow(tableName, row.key(), c, row.getBytes(c), ifColumn, equals, pTablePath);
					if (!isPut)
						break;
				}
//            			putWholeRow(tableName, row, ifColumn, equals, pTablePath);
			}
			System.out.println("KVS worker ends putting " + count + " rows");
			logger.info("KVS worker ends putting " + count + " rows");
			String content = isPut ? "OK" : "FAIL";
			rsp.status(200, content);
			rsp.type("text/html");
			rsp.header("Content-Length", String.valueOf(content.length()));
			rsp.bodyAsBytes(content.getBytes());
			return null;
		});

		put("/data/:T", (req, rsp) -> {
			hasReceiveRequest = true;
			String tableName = req.params("T");
			String ifColumn = req.queryParams("ifcolumn");
			String equals = req.queryParams("equals");
			String pTablePath = directory + File.separator + tableName + ".table";
			ByteArrayInputStream bis = new ByteArrayInputStream(req.bodyAsBytes());
			Row row;
			boolean isPut = true;
			int count = 0;
			while ((row = Row.readFrom(bis)) != null) {
				count++;
				isPut = putWholeRow(tableName, row, ifColumn, equals, pTablePath);
			}
			System.out.println("KVS worker ends putting " + count + " rows");
			logger.info("KVS worker ends putting " + count + " rows");
			String content = isPut ? "OK" : "FAIL";
			rsp.status(200, content);
			rsp.type("text/html");
			rsp.header("Content-Length", String.valueOf(content.length()));
			rsp.bodyAsBytes(content.getBytes());
			return null;
		});

		put("/rename/:T", (req, rsp) -> {
			hasReceiveRequest = true;
			String tableName = req.params("T");
			String newTableName = req.body();
			String content;
			Map<String, Row> table = tables.get(tableName);
			Map<String, persistRow> pTable = persistTables.get(tableName);
			if (table == null && pTable == null) {
				content = "Not Found";
				rsp.status(404, content);
				rsp.header("Content-Length", String.valueOf(content.length()));
				rsp.bodyAsBytes(content.getBytes());
			} else if (tables.containsKey(newTableName) || persistTables.containsKey(newTableName)) {
				content = "Conflict";
				rsp.status(409, content);
				rsp.header("Content-Length", String.valueOf(content.length()));
				rsp.bodyAsBytes(content.getBytes());
			} else {
				content = "OK";
				rsp.status(200, content);
				if (table != null) {
					tables.put(newTableName, tables.remove(tableName));
				} else {
					persistTables.put(newTableName, persistTables.remove(tableName));
				}
				String pTablePath = directory + File.separator + tableName + ".table";
				String destPath = directory + File.separator + newTableName + ".table";
				File rafFile = new File(pTablePath);
				File destFile = new File(destPath);
				if (!rafFile.renameTo(destFile)) {
					logger.error("rename table failed: " + pTablePath + " -> " + destPath);
				} else {
					tableFileLoc.put(newTableName, new RandomAccessFile(destFile, "rw"));
				}
			}
			rsp.type("text/plain");
			rsp.header("Content-Length", String.valueOf(content.length()));
			rsp.bodyAsBytes(content.getBytes());
			return null;
		});

		put("/delete/:T", (req, rsp) -> {
			hasReceiveRequest = true;
			String tableName = req.params("T");
			String content;
			Map<String, Row> table = tables.get(tableName);
			Map<String, persistRow> pTable = persistTables.get(tableName);
			if (table == null && pTable == null) {
				content = "Not Found";
				rsp.status(404, content);
				rsp.header("Content-Length", String.valueOf(content.length()));
				rsp.bodyAsBytes(content.getBytes());
			} else {
				content = "OK";
				rsp.status(200, content);
				String pTablePath = directory + File.separator + tableName + ".table";
				deleteTable(tableName, pTablePath);
			}
			rsp.type("text/plain");
			rsp.header("Content-Length", String.valueOf(content.length()));
			rsp.bodyAsBytes(content.getBytes());
			return null;
		});

		get("/data/:T/:R/:C", (req, rsp) -> {
			hasReceiveRequest = true;
			String tableName = req.params("T");
			String rowName = req.params("R");
			String colName = req.params("C");
			byte[] value = getRow(tableName, rowName, colName);
			rsp.status(200, "OK");
			rsp.type("text/plain");
			if (value != null) {
				rsp.header("Content-Length", String.valueOf(value.length));
				rsp.bodyAsBytes(value);
			} else {
				String content = "Not Found";
				rsp.status(404, content);
				rsp.header("Content-Length", String.valueOf(content.length()));
				rsp.bodyAsBytes(content.getBytes());
			}
			return null;
		});
		
		get("/exist/:T/:R", (req, rsp) -> {
			hasReceiveRequest = true;
			String tableName = req.params("T");
			String rowName = req.params("R");
			boolean exist = existsRow(tableName, rowName);
			rsp.status(200, "OK");
			rsp.type("text/plain");
			String content = "Found";
			if (exist) {
//				System.out.println(tableName +" "+rowName+" exits");
				rsp.header("Content-Length", String.valueOf(content.length()));
				rsp.bodyAsBytes(content.getBytes());
			} else {
//				System.out.println(tableName +" "+rowName+" not exits");
				content = "Not Found";
				rsp.status(404, content);
				rsp.header("Content-Length", String.valueOf(content.length()));
				rsp.bodyAsBytes(content.getBytes());
			}
			return null;
		});

		get("/data/:T/:R", (req, rsp) -> {
			hasReceiveRequest = true;
			String tableName = req.params("T");
			String rowName = req.params("R");
			byte[] value = getWholeRow(tableName, rowName);
			rsp.status(200, "OK");
			rsp.type("text/plain");
			if (value != null) {
				rsp.header("Content-Length", String.valueOf(value.length));
				rsp.bodyAsBytes(value);
			} else {
				String content = "Not Found";
				rsp.status(404, content);
				rsp.header("Content-Length", String.valueOf(content.length()));
				rsp.bodyAsBytes(content.getBytes());
			}
			return null;
		});

		get("/data/:T", (req, rsp) -> {
			hasReceiveRequest = true;
			String tableName = req.params("T");
			String startRow = req.queryParams("startRow");
			String endRowExclusive = req.queryParams("endRowExclusive");
			rsp.status(200, "OK");
			rsp.type("text/plain");
			Map<String, Row> table = tables.get(tableName);
			Map<String, persistRow> pTable = persistTables.get(tableName);
			if (table == null && pTable == null) {
				String content = "Not Found";
				rsp.status(404, content);
//                rsp.header("Content-Length", String.valueOf(content.length()));
				rsp.bodyAsBytes(content.getBytes());
				return null;
			}
			Set<String> keySet = table != null ? table.keySet() : pTable.keySet();
			boolean isRowNull = true;
			for (String rowName : keySet) {
				if ((startRow == null || startRow.compareTo(rowName) <= 0)
						&& (endRowExclusive == null || rowName.compareTo(endRowExclusive) < 0)) {
					byte[] value = getWholeRow(tableName, rowName);
					if (value != null) {
						rsp.write(value);
						rsp.write("\n".getBytes());
						isRowNull = false;
					}
				}
			}
			if (!isRowNull) {
				rsp.write("\n".getBytes());
			}
			return null;
		});

		get("/count/:T", (req, rsp) -> {
			hasReceiveRequest = true;
			String tableName = req.params("T");
			Map<String, Row> table = tables.get(tableName);
			Map<String, persistRow> pTable = persistTables.get(tableName);
			rsp.status(200, "OK");
			rsp.type("text/html");
			String content;
			if (table == null && pTable == null) {
				content = "Not Found";
				rsp.status(404, content);
			} else {
				content = String.valueOf(table != null ? table.size() : pTable.size());
			}
			rsp.header("Content-Length", String.valueOf(content.length()));
			rsp.bodyAsBytes(content.getBytes());
			return null;
		});

		get("/view/:T", (req, rsp) -> {
			hasReceiveRequest = true;
			String tableName = req.params("T");
			String fromRow = req.queryParams("fromRow");
			String pageNumber = req.queryParams("pageNumber");
			pageNumber = pageNumber != null ? pageNumber : "1";
			rsp.type("text/html");
			Map<String, Row> table = tables.get(tableName);
			Map<String, persistRow> pTable = persistTables.get(tableName);
			if (table == null && pTable == null) {
				rsp.status(404, "Not Found");
				return null;
			}
			Set<String> keySet = table != null ? table.keySet() : pTable.keySet();
			Vector<String> rowNames = new Vector<>();
			for (String rowName : keySet) {
				if (fromRow == null || fromRow.compareTo(rowName) <= 0) {
					rowNames.add(rowName);
				}
			}
			rowNames.sort(null);
			RandomAccessFile raf = tableFileLoc.get(tableName);
			Vector<Row> rows = new Vector<>();
			for (int i = 0; i < Math.min(rowNames.size(), numPageShow); i++) {
				if (table != null) {
					rows.add(table.get(rowNames.get(i)));
				} else {
					synchronized (raf) {
						raf.seek(pTable.get(rowNames.get(i)).offset);
						rows.add(Row.readFrom(raf));
					}
				}
			}
			String html = "<html><head><title>Table View</title></head>";
			html += "<body><table>";
			html += "<style> table, th, td {border: 1px solid black;} p, div, footer {text-align:right} </style>";
			html += "<caption>Table:" + tableName + "(Page No." + pageNumber + ")" + "</caption>";
			html += "<tr> <td> </td>";
			Set<String> colNameSet = new HashSet<>();
			for (Row row : rows) {
				colNameSet.addAll(row.columns());
			}
			Vector<String> colNameVec = new Vector<>();
			for (String col : colNameSet) {
				for (Row row : rows) {
					if (row.get(col) != null) {
						colNameVec.add(col);
						break;
					}
				}
			}
			colNameVec.sort(null);
			for (int j = 0; j < colNameVec.size(); j++) {
				html += "<td>" + colNameVec.get(j) + "</td>";
			}
			html += "</tr>";
			for (Row row : rows) {
				html += "<tr> <td>" + row.key + "</td>";
				for (int j = 0; j < colNameVec.size(); j++) {
					String value = row.get(colNameVec.get(j));
					html += "<td>" + (value != null ? value : "") + "</td>";
				}
				html += "</tr>";
			}
			html += "</table>";
			boolean hasNextPage = rowNames.size() > numPageShow;
			if (hasNextPage) {
				html += "<p><a href=/view/" + tableName + "?fromRow=" + rowNames.get(numPageShow) + "&pageNumber="
						+ (Integer.valueOf(pageNumber) + 1) + ">Next</a></p>";
			}
			html += "</body></html>";
			return html;
		});

		get("/", (req, rsp) -> {
			hasReceiveRequest = true;
			String workerAddr = remoteHost.split(":")[0] + ":" + workerPort;
			String html = "<html><head><title>KVS Worker</title></head>";
			html += "<body><table>";
			html += "<style> table, th, td {border: 1px solid black;} </style>";
			html += "<tr> <td>tableName</td> <td>numRows</td> <td>isPersistent</td> </tr>";
			for (String tableName : tables.keySet()) {
				Map<String, Row> table = tables.get(tableName);
				html += getTableSummary(workerAddr, tableName, table.size(), false);
			}
			for (String tableName : persistTables.keySet()) {
				Map<String, persistRow> table = persistTables.get(tableName);
				html += getTableSummary(workerAddr, tableName, table.size(), true);
			}
			html += "</table></body></html>";
			rsp.status(200, "OK");
			rsp.type("text/html");
			rsp.header("Content-Length", String.valueOf(html.length()));
			rsp.body(html);
			return null;
		});

		System.out.println("kvs worker " + workerID + " working serving.");
		logger.info("kvs worker " + workerID + " working serving.");
	}

	public static String getTableSummary(String workerAddr, String tableName, int tableSize, boolean isPersistent) {
		String tableSummary = "<tr>";
		// tableSummary += "<td><a href=http://" + workerAddr + "/view/" + tableName +
		// "/>" + tableName + "</a></td>";
		tableSummary += "<td><a href=/view/" + tableName + "/>" + tableName + "</a></td>";
		tableSummary += "<td>" + tableSize + "</td>";
		tableSummary += "<td>" + (isPersistent ? "persistent" : "") + "</td>";
		tableSummary += "</tr>";
		return tableSummary;
	}

	public static boolean putRow(String tableName, String rowName, String colName, byte[] value, String ifColumn,
			String equals, String pTablePath) {
		if (!persistTables.containsKey(tableName)) {
			return putNormalRow(tableName, rowName, colName, value, ifColumn, equals, pTablePath);
		}
		return putPersistentRow(tableName, rowName, colName, value, ifColumn, equals);
	}

	public static boolean putWholeRow(String tableName, Row rowData, String ifColumn, String equals,
			String pTablePath) {
		if (!persistTables.containsKey(tableName)) {
			return putWholeNormalRow(tableName, rowData, ifColumn, equals, pTablePath);
		}
		return putWholePersistentRow(tableName, rowData, ifColumn, equals);
	}

	public static boolean putWholeNormalRow(String tableName, Row rowData, String ifColumn, String equals,
			String pTablePath) {
		try {
			// create table & raf file if not exist
			if (!tables.containsKey(tableName)) {
				tables.put(tableName, new ConcurrentHashMap<>());
			}
			if (!tableFileLoc.containsKey(tableName)) {
				tableFileLoc.put(tableName, new RandomAccessFile(pTablePath, "rw"));
			}
			Map<String, Row> table = tables.get(tableName);
			if (ifColumn != null && equals != null) {
				String valCheck = rowData.get(ifColumn);
				if (valCheck == null || !valCheck.equals(equals)) {
					return false;
				}
			}
			table.put(rowData.key, rowData);
			tables.put(tableName, table);

		} catch (Exception e) {
			System.out.println(e.getMessage());
			logger.error("err puting whole normal row: " + e.getMessage());
		}
		return true;
	}

	public static boolean putWholePersistentRow(String tableName, Row rowData, String ifColumn, String equals) {
		try {
			// raf file should already exist
			Map<String, persistRow> pTable = persistTables.get(tableName);
			RandomAccessFile raf = tableFileLoc.get(tableName);
			Row row;
			persistRow pRow;
			String rowName = rowData.key;
			if (!pTable.containsKey(rowName)) {
				pRow = new persistRow(rowName);
				row = new Row(pRow.getKey());
			} else {
				pRow = pTable.get(rowName);
				synchronized (raf) {
					raf.seek(pRow.getOffset());
					row = Row.readFrom(raf);
				}
			}
			if (ifColumn != null && equals != null) {
				String valCheck = row.get(ifColumn);
				if (valCheck == null || !valCheck.equals(equals)) {
					return false;
				}
			}
			synchronized (raf) {
				pRow.setOffset(raf.length());
				pTable.put(rowName, pRow);
				persistTables.put(tableName, pTable);

				raf.seek(raf.length());
				raf.write(rowData.toByteArray());
				raf.write("\n".getBytes());
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
			logger.error("err puting whole persistent row: " + e.getMessage());
		}
		return true;
	}

	public static boolean putNormalRow(String tableName, String rowName, String colName, byte[] value, String ifColumn,
			String equals, String pTablePath) {
		try {
			// create table & raf file if not exist
			if (!tables.containsKey(tableName)) {
				tables.put(tableName, new ConcurrentHashMap<>());
			}
			if (!tableFileLoc.containsKey(tableName)) {
				tableFileLoc.put(tableName, new RandomAccessFile(pTablePath, "rw"));
			}
			Map<String, Row> table = tables.get(tableName);
			Row row = table.getOrDefault(rowName, new Row(rowName));
			if (ifColumn != null && equals != null) {
				String valCheck = row.get(ifColumn);
				if (valCheck == null || !valCheck.equals(equals)) {
					return false;
				}
			}
//            RandomAccessFile raf = tableFileLoc.get(tableName);
//            synchronized(raf) {
//                long newPos = raf.length();

			row.put(colName, value);
			table.put(rowName, row);
			tables.put(tableName, table);

//                raf.seek(newPos);
//                raf.write(row.toByteArray());
//                raf.write("\n".getBytes());
//            }
		} catch (Exception e) {
			logger.error("put normal error: " + e.getMessage());
		}
		return true;
	}

	public static boolean putPersistentRow(String tableName, String rowName, String colName, byte[] value,
			String ifColumn, String equals) {
		try {
			// raf file should already exist
			Map<String, persistRow> pTable = persistTables.get(tableName);
			RandomAccessFile raf = tableFileLoc.get(tableName);
			Row row;
			persistRow pRow;
			if (!pTable.containsKey(rowName)) {
				pRow = new persistRow(rowName);
				row = new Row(pRow.getKey());
			} else {
				pRow = pTable.get(rowName);
				synchronized (raf) {
					raf.seek(pRow.getOffset());
					row = Row.readFrom(raf);
				}
			}
			if (ifColumn != null && equals != null) {
				String valCheck = row.get(ifColumn);
				if (valCheck == null || !valCheck.equals(equals)) {
					return false;
				}
			}
			synchronized (raf) {
				pRow.setOffset(raf.length());
				pTable.put(rowName, pRow);
				persistTables.put(tableName, pTable);

				row.put(colName, value);

				raf.seek(raf.length());
				raf.write(row.toByteArray());
				raf.write("\n".getBytes());
			}
		} catch (Exception e) {
			logger.error("put persist error: " + e.getMessage());
		}
		return true;
	}
	
	public static boolean existsRow(String tableName, String rowName) {
		// persist table
		if(persistTables.containsKey(tableName)) {
			Map<String, persistRow> table = persistTables.get(tableName);
			if (!table.containsKey(rowName)) return false;
			else return true;
		}
		// normal table
		else {
			if (!tables.containsKey(tableName)) return false;
			else{
				if(!tables.get(tableName).containsKey(rowName)) return false;
				return true;
			}
		}
	}

	public static byte[] getRow(String tableName, String rowName, String colName) {
		Row row = persistTables.containsKey(tableName) ? getPersistentRow(tableName, rowName)
				: getNormalRow(tableName, rowName);
		return row != null ? row.getBytes(colName) : null;
	}

	public static byte[] getWholeRow(String tableName, String rowName) {
		Row row = persistTables.containsKey(tableName) ? getPersistentRow(tableName, rowName)
				: getNormalRow(tableName, rowName);
		return row != null ? row.toByteArray() : null;
	}

	public static Row getNormalRow(String tableName, String rowName) {
		if (!tables.containsKey(tableName)) {
			return null;
		}
		Map<String, Row> table = tables.get(tableName);
		return table.get(rowName);
	}

	public static Row getPersistentRow(String tableName, String rowName) {
		try {
			Map<String, persistRow> table = persistTables.get(tableName);
			if (!table.containsKey(rowName)) {
				return null;
			}
			persistRow pRow = table.get(rowName);
			RandomAccessFile raf = tableFileLoc.get(tableName);
			Row row;
			synchronized (raf) {
				raf.seek(pRow.getOffset());
				row = Row.readFrom(raf);
			}
			return row;
		} catch (Exception e) {
			logger.error("get persist error. table name: " + tableName + ", row name:" + rowName + ": " + e.getMessage());
		}
		return null;
	}

	public static void deleteTable(String tableName, String pTablePath) {
		if (tables.containsKey(tableName)) {
			tables.remove(tableName);
		} else {
			persistTables.remove(tableName);
		}
		try {
			tableFileLoc.remove(tableName);
			Files.deleteIfExists(Paths.get(pTablePath));
		} catch (Exception e) {
			logger.error("delete error: " + e.getMessage());
		}
	}

//	public static void startGarbageCollector(String directory) {
//		Thread thread = new Thread(new Runnable() {
//			@Override
//			public void run() {
//				try {
//					while (true) {
//						Thread.sleep(10000);
//						if (!hasReceiveRequest) {
//							Vector<String> allTableNames = new Vector<>();
//							for (String tableName : tables.keySet()) {
//								allTableNames.add(tableName);
//							}
//							for (String tableName : persistTables.keySet()) {
//								allTableNames.add(tableName);
//							}
//							for (String tableName : allTableNames) {
//								FileLock fLock = tableFileLoc.get(tableName).getChannel().lock();
//								Map<String, Row> table = tables.get(tableName);
//								Map<String, persistRow> pTable = persistTables.get(tableName);
//								Set<String> rowNames = table != null ? table.keySet() : pTable.keySet();
//								String tableLoc = directory + File.separator + tableName + ".table";
//								String tableLocTemp = directory + File.separator + tableName + ".table2";
//								RandomAccessFile newRaf = new RandomAccessFile(tableLocTemp, "rw");
//								for (String rowName : rowNames) {
//									synchronized (newRaf) {
//										newRaf.seek(newRaf.length());
//										byte[] value = getWholeRow(tableName, rowName);
//										if (pTable != null) {
//											pTable.get(rowName).setOffset(newRaf.length());
//										}
//										newRaf.write(value);
//										newRaf.write("\n".getBytes());
//									}
//								}
//								// close & delele & rename old raf
//								fLock.release();
//								tableFileLoc.get(tableName).close();
//								try {
//									Files.deleteIfExists(Paths.get(tableLoc));
//								} catch (Exception e) {
//									logger.error(e.getMessage());
//								}
//								if (!(new File(tableLocTemp)).renameTo(new File(tableLoc))) {
//									logger.error("rename failed.");
//								} else {
//									tableFileLoc.put(tableName, new RandomAccessFile(new File(tableLoc), "rw"));
//								}
//							}
//						}
//						hasReceiveRequest = false;
//					}
//				} catch (Exception e) {
//					logger.error(e.getMessage());
//				}
//			}
//		});
//		thread.start();
//	}
}