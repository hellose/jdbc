package study.jdbc.meta;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class JdbcColumnMetaDto {
	private String name;
	private String remarks;

	private Integer dataTypeInt;
	private String dataTypeName;

	private boolean nullable;

	private Integer decimalDigits;
	private Integer columnSize;

	private String involvedSchema;
	private String involvedTable;
}
