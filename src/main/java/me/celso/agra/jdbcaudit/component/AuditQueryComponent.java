package me.celso.agra.jdbcaudit.component;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.groupingBy;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.DigestUtils;

@Component
public class AuditQueryComponent implements CommandLineRunner {
	
	@Autowired
	JdbcTemplate JdbcTemplate;
	
	String SQL = "select c.*, pgd.description from information_schema.columns c " + 
			"inner join pg_catalog.pg_statio_all_tables as st on c.table_schema = st.schemaname and c.table_name = st.relname " + 
			"left join pg_catalog.pg_description pgd on pgd.objoid=st.relid and pgd.objsubid=c.ordinal_position " + 
			"where c.table_schema = ?;";

	@Override
	public void run(String... args) throws Exception {
		List<Map<String, Object> > results = JdbcTemplate.queryForList(SQL, new Object[] { "cadastro_cidadao" });
		results.forEach(System.out::println);
		
		Map<String, Map<String, Map<String, List<Map<String, Object>>>>> tables = results.parallelStream()
				.map(map -> {
					String id = String.format("%s_%s_%s_%s", 
							map.get("table_catalog").toString(), map.get("table_schema").toString(), map.get("table_name").toString(), map.get("column_name").toString());
					id = DigestUtils.md5DigestAsHex(id.getBytes());
					map.put("id", id);
					return map;
				})
				.collect(
						groupingBy( db -> {
									return db.get("table_catalog").toString();
								}, groupingBy(schema -> {
									return schema.get("table_schema").toString();
								}, groupingBy(table -> {
									return table.get("table_name").toString();
								})
										
								)
						)
						
				);
		JSONObject json = new JSONObject(tables);
		
		System.out.println(json);
		
//		Set<String> keys = tables.keySet();
//		for (String key : keys) {
//			System.out.println(tables.get(key));
//		}
		
//		results.stream().collect(
//				Collectors.groupingBy( l1 -> l1.get,
//                Collectors.flatMapping(item -> item.getSubItems().stream(),
//                        Collectors.groupingBy(SubItem::getKey2))));
		
	}
	
	
}
