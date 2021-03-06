package me.celso.agra.jdbcaudit.component;

import static java.util.stream.Collectors.groupingBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
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
		
		Map<String, Object> mapToJson = new HashMap<String, Object>();
		mapToJson.
		JSONObject json = new JSONObject(tables);
		System.out.println(json);

		ClientConfiguration clientConfiguration = ClientConfiguration.builder().connectedTo("localhost:9200").build();
		RestHighLevelClient client = RestClients.create(clientConfiguration).rest();

		IndexRequest request = new IndexRequest("database");
		request.source(json, XContentType.JSON);

		IndexResponse response = client.index(request, RequestOptions.DEFAULT);
		String index = response.getIndex();
		long version = response.getVersion();
		
		System.out.println(index);
		System.out.println(version);
	}
	
	
}
