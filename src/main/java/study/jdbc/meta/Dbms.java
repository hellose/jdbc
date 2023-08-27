package study.jdbc.meta;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum Dbms {

	mysql("jdbc:mysql://localhost:3306/classicmodels", "root", "1234", ParsingType.DATABASE_IS_EQUAL_SCHEMA, null),
	postgresql("jdbc:postgresql://localhost:5432/dvdrental", "postgres", "1234",
			ParsingType.DATABASE_IS_NOT_EQUAL_SCHEMA, TableSearchConversion.LOWERCASE),
	oracle("jdbc:oracle:thin:@localhost:1522/ORCL", "sys as sysdba", "1234", ParsingType.DATABASE_IS_NOT_EQUAL_SCHEMA,
			TableSearchConversion.UPPERCASE),
	h2("jdbc:h2:~/test", "sa", "", ParsingType.DATABASE_IS_NOT_EQUAL_SCHEMA, TableSearchConversion.UPPERCASE);

	// JDBC 호출 메서드 결정 enum
	private static enum ParsingType {
		// MySQL
		DATABASE_IS_EQUAL_SCHEMA("TABLE_CAT"), // -> DatabaseMetaData.getCatalogs()
		// Oracle, PostgreSQL, H2
		DATABASE_IS_NOT_EQUAL_SCHEMA("TABLE_SCHEM"); // -> DatabaseMetaData.getSechemas()

		private final String rsColumnName;

		private ParsingType(String rsColumnName) {
			this.rsColumnName = rsColumnName;
		}
	}

	// 특정 스키마에서 테이블 검색시 vendor에 맞는 대소문자 String 변환 필요
	private static enum TableSearchConversion {
		UPPERCASE, LOWERCASE
	}

	@Getter
	private final String vendorName;

	private final ParsingType parsingType;
	private final TableSearchConversion tableSearchConversion;

	private Connection con;

	private Dbms(String url, String user, String password, ParsingType parsingType,
			TableSearchConversion tableSearchConversion) {
		this.parsingType = parsingType;
		this.tableSearchConversion = tableSearchConversion;

		try {
			con = DriverManager.getConnection(url, user, password);
			this.vendorName = con.getMetaData().getDatabaseProductName();

		} catch (SQLException e) {
			throw new RuntimeException("[ERROR] Dbms enum Constructor: " + url);
		}
	}

	/*
	 * Database 제품 정보 출력
	 */
	public void printDatabaseProductInformation() throws SQLException {
		DatabaseMetaData dbMeta = con.getMetaData();
		log.debug("----- Database Product Information (" + vendorName + ") -----");

		log.debug("databaseProductName: {}", dbMeta.getDatabaseProductName());
		log.debug("databaseProductVersion: {}", dbMeta.getDatabaseProductVersion());
		log.debug("databaseMajorVersion: {}", dbMeta.getDatabaseMajorVersion());
		log.debug("databaseMinorVersion: {}", dbMeta.getDatabaseMinorVersion());
		log.debug("------------------------------");
	}

	/*
	 * JDBC Driver 정보 출력
	 */
	public void printJdbcDriverInfo() throws SQLException {
		DatabaseMetaData dbMeta = con.getMetaData();
		log.debug("----- Driver Information (" + vendorName + ") -----");

		log.debug("driverName: {}", dbMeta.getDriverName());
		log.debug("driverVersion: {}", dbMeta.getDriverVersion());
		log.debug("driverMajorVersion: {}", dbMeta.getDriverMajorVersion());
		log.debug("driverMinorVersion: {}", dbMeta.getDriverMinorVersion());
		log.debug("------------------------------");
	}

	/*
	 * Connection 기본 정보와 타겟으로 설정되어있는 catalog, schema 출력
	 */
	public void printConnectionInformation() throws SQLException {
		log.debug("----- Connection Info (" + vendorName + ") -----");

		log.debug("connection class name: {}", con.getClass().getName());

		if (this.parsingType == ParsingType.DATABASE_IS_EQUAL_SCHEMA) {
			log.debug("current catalog: {}", con.getSchema());
			log.debug("current schema: {}", con.getCatalog());
		} else {
			log.debug("current catalog: {}", con.getCatalog());
			log.debug("current schema: {}", con.getSchema());
		}

		log.debug("------------------------------");
	}

	/*
	 * (print 포함) 데이터베이스의 스키마 리스트
	 */
	public List<String> getSchemaList() throws SQLException {
		log.debug("----- Schema List (" + vendorName + ") -----");

		List<String> schemas = new ArrayList<>();

		ResultSet rs = null;
		if (this.parsingType == ParsingType.DATABASE_IS_EQUAL_SCHEMA) {
			rs = con.getMetaData().getCatalogs();
		} else {
			rs = con.getMetaData().getSchemas();
		}

		while (rs.next()) {
			schemas.add(rs.getString(this.parsingType.rsColumnName));
		}
		rs.close();
		rs = null;

		schemas.forEach(schemaName -> {
			log.debug("{}", schemaName);
		});

		return schemas;
	}

	/*
	 * (print 포함)
	 * 데이터베이스 특정 schemaName 내부에서 -> 특정 objectFilter 오브젝트 타입에 해당하며 -> tableNamePattern 패턴에 해당하는 것들
	 */
	public List<JdbcTableMetaDto> getTables(String schemaName, String tableNamePattern, Boolean tableOrViewOrAll)
			throws SQLException {

		// "SYSTEM INDEX", "SYSTEM TABLE", "TABLE", "VIEW"... 에서 "TABLE", "VIEW"만 필터링
		String[] objectFilter = null;

		if (tableOrViewOrAll == null) {
			objectFilter = new String[] { "TABLE", "VIEW" };
		} else if (tableOrViewOrAll.booleanValue() == true) {
			objectFilter = new String[] { "TABLE" };
		} else {
			objectFilter = new String[] { "VIEW" };
		}

		DatabaseMetaData meta = con.getMetaData();
		ResultSet rs = null;
		// vendor에 맞게 대소문자 변경
		String changedTableNamePattern = changeTablePattern(tableNamePattern);

		// vendor에 맞게 인자 전달
		if (this.parsingType == ParsingType.DATABASE_IS_EQUAL_SCHEMA) {
			rs = meta.getTables(schemaName, null, "%" + changedTableNamePattern + "%", objectFilter);
		} else {
			rs = meta.getTables(null, schemaName, "%" + changedTableNamePattern + "%", objectFilter);
		}

		List<JdbcTableMetaDto> tables = new ArrayList<>();

		while (rs.next()) {
			JdbcTableMetaDto t = new JdbcTableMetaDto();

			t.setName(rs.getString("TABLE_NAME"));
			t.setType(rs.getString("TABLE_TYPE"));
			// "REMARKS" -> 테이블인 경우 테이블 코멘트
			t.setRemarks(rs.getString("REMARKS"));

			if (this.parsingType == ParsingType.DATABASE_IS_EQUAL_SCHEMA) {
				t.setInvolvedSchema(rs.getString("TABLE_CAT"));
			} else {
				t.setInvolvedSchema(rs.getString("TABLE_SCHEM"));
			}

			tables.add(t);
		}
		log.debug("\n");

		rs.close();
		rs = null;

		return tables;
	}

	private String changeTablePattern(String tableNamePattern) {
		if (tableNamePattern == null) {
			return "";
		}

		TableSearchConversion conversion = this.tableSearchConversion;
		if (conversion == null) {
			return "";
		} else if (conversion == TableSearchConversion.UPPERCASE) {
			return tableNamePattern.toUpperCase();
		} else {
			return tableNamePattern.toLowerCase();
		}
	}

	/*
	 * 데이터베이스 특정 스키마 특정 테이블의 모든 컬럼 정보
	 */
	public List<JdbcColumnMetaDto> getColumns(String schemaName, String tableName) throws SQLException {
		DatabaseMetaData meta = con.getMetaData();

		ResultSet rs = null;

		// vendor에 맞게 인자 전달
		if (parsingType == ParsingType.DATABASE_IS_EQUAL_SCHEMA) {
			rs = meta.getColumns(schemaName, null, tableName, null);
		} else if (parsingType == ParsingType.DATABASE_IS_NOT_EQUAL_SCHEMA) {
			rs = meta.getColumns(null, schemaName, tableName, null);
		}

		List<JdbcColumnMetaDto> columns = new ArrayList<>();

		while (rs.next()) {
			JdbcColumnMetaDto c = new JdbcColumnMetaDto();

			c.setName(rs.getString("COLUMN_NAME"));
			c.setRemarks(rs.getString("REMARKS"));
			c.setColumnSize(rs.getInt("COLUMN_SIZE"));
			c.setDecimalDigits(rs.getInt("DECIMAL_DIGITS"));
			c.setDataTypeInt(rs.getInt("DATA_TYPE"));
			c.setDataTypeName(rs.getString("TYPE_NAME"));
			c.setInvolvedTable("TABLE_NAME");

			int nullable = rs.getInt("NULLABLE");
			if (nullable == 0) {
				c.setNullable(false);
			} else {
				c.setNullable(true);
			}

			if (this.parsingType == ParsingType.DATABASE_IS_EQUAL_SCHEMA) {
				c.setInvolvedSchema(rs.getString("TABLE_CAT"));
			} else {
				c.setInvolvedSchema(rs.getString("TABLE_SCHEM"));
			}

			columns.add(c);
		}
		log.debug("\n");

		rs.close();
		rs = null;

		return columns;
	}

	/*
	 * 디버깅 - 인덱스를 통해 전체 열 출력
	 */
	public static void printResultSet(ResultSet resultSet) throws SQLException {

		ResultSetMetaData rsMeta = resultSet.getMetaData();
		// ResultSet 열 전체 개수
		int resultSetColumnCount = rsMeta.getColumnCount();

		// ResultSet 각 열이름이 들어있는 배열
		String[] resultSetColumnNames = new String[resultSetColumnCount];
		for (int i = 1; i <= resultSetColumnCount; i++) {
			resultSetColumnNames[i - 1] = rsMeta.getColumnName(i);
		}

		StringBuilder sb = new StringBuilder();

		int rowNum = 1;
		while (resultSet.next()) {
			// row number
			sb.append("[").append(rowNum).append("] ");

			// column's value
			for (int i = 0; i < resultSetColumnCount; i++) {
				sb.append(resultSetColumnNames[i] + "(" + (i + 1) + "): ").append(resultSet.getString(i + 1));
				if (i != resultSetColumnCount - 1) {
					sb.append(", ");
				}
			}
			log.debug("{}", sb.toString());
			sb.setLength(0);
			rowNum++;
		}
		log.debug("\n");
	}

	/*
	 * 디버깅 - ResultSet열 이름 리스트에 해당하는 것만 출력
	 */
	public static void printSpecificColumnOfResultSet(ResultSet rs, String... resultSetColumnFilter)
			throws SQLException {

		if ((resultSetColumnFilter == null) || (resultSetColumnFilter.length == 0)) {
			printResultSet(rs);
			return;
		}

		ResultSetMetaData rsMeta = rs.getMetaData();
		int resultSetColumnCount = rsMeta.getColumnCount();

		StringBuilder sb = new StringBuilder();

		int rowNum = 1;
		while (rs.next()) {
			// row number
			sb.append("[").append(rowNum).append("] ");

			// column's value
			for (int i = 0; i < resultSetColumnFilter.length; i++) {
				sb.append(resultSetColumnFilter[i] + ": ");
				sb.append(rs.getString(resultSetColumnFilter[i]));
				if (i != resultSetColumnCount - 1) {
					sb.append(", ");
				}
			}
			log.debug("\n");
			sb.setLength(0);

			rowNum++;
		}
		log.debug("\n");
	}

	private static void close(Connection con) {
		if (con != null) {
			try {
				con.close();
			} catch (SQLException ex) {
				log.debug("[ERROR] Could not close JDBC Connection: {}", con.getClass().getName());
			} catch (Throwable ex) {
				log.debug("[ERROR] Unexpected exception on closing JDBC Connection", ex);
			}
		}
	}

	public static void closeAll() {
		Dbms[] dbs = Dbms.values();
		for (Dbms db : dbs) {
			Dbms.close(db.con);
			db.con = null;
		}
	}

	public Connection getConnection() {
		return this.con;
	}

}
