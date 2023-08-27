package study.jdbc.meta;

import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestMethodOrder;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@TestInstance(Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class JdbcMetaDtoTest {

	private Dbms[] dbs;

	@BeforeAll
	void beforeAll() {
		dbs = Dbms.values();
	}

	@AfterAll
	void closeAll() {
		Dbms.closeAll();
	}

	@Test
	@Order(100)
	void databaseProductInfo() {
		try {
			for (Dbms db : dbs) {
				db.printDatabaseProductInformation();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	@Order(200)
	void jdbcDriverInfo() {
		try {
			for (Dbms db : dbs) {
				db.printJdbcDriverInfo();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	@Order(300)
	void connectionBasicInfo() {
		try {
			for (Dbms db : dbs) {
				db.printConnectionInformation();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	@Order(400)
	void printSchema() {
		try {
			for (Dbms db : dbs) {
				db.getSchemaList();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	@Order(500)
	void getTables() {
		try {
			Dbms db = Dbms.postgresql;
			List<String> schemas = db.getSchemaList();

			for (String schemaName : schemas) {
				List<JdbcTableMetaDto> tables = db.getTables(schemaName, null /* 모든 이름 조건 */, null /* 테이블과 뷰 모두 */);
				tables.forEach(t -> log.debug(t.toString()));
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	@Order(600)
	void getColumns() {
		try {
			Dbms db = Dbms.postgresql;
			List<JdbcColumnMetaDto> columns = db.getColumns("public" /* 스키마명 */, "customer" /* 테이블명 */);
			columns.forEach(c -> log.debug(c.toString()));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
