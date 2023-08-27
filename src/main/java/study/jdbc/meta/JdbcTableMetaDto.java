package study.jdbc.meta;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class JdbcTableMetaDto {

	private String name;
	private String type;
	private String remarks;
	private String involvedSchema;
}
