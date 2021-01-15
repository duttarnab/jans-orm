package io.jans.orm.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.SimpleExpression;
import com.querydsl.core.types.dsl.Wildcard;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.MySQLTemplates;
import com.querydsl.sql.RelationalPathBase;
import com.querydsl.sql.SQLQuery;
import com.querydsl.sql.SQLQueryFactory;
import com.querydsl.sql.SQLTemplates;

public class Test {
	public static void main(String args[]) {
		try {
			DataSource dataSource = setupDataSource();

			Connection con = dataSource.getConnection();
			
			SQLTemplates templates = MySQLTemplates.builder()
				    .printSchema()
				    .build();
			Configuration configuration = new Configuration(templates);
			SQLQueryFactory sqlQueryFactory = new SQLQueryFactory(configuration, dataSource);

			String table = "client";
			Path<String> tablePath = ExpressionUtils.path(String.class, table);
			Path<String> docAlias = ExpressionUtils.path(String.class, "abs");
			SimpleExpression<String> usernamePath = Expressions.as(tablePath, docAlias);
			RelationalPathBase<Object> tableRelationalPath = new RelationalPathBase<>(Object.class, "doc2", "gluu", "client");
			
			@SuppressWarnings("rawtypes")
//			Expression<String> allPath = Expressions.template(cl, template, args)("*");
			NumberPath<Long> idPath = Expressions.numberPath(Long.class, docAlias, "id");
			
			SQLQuery sqlQuery = sqlQueryFactory.select(Expressions.list(Expressions.as(ExpressionUtils.count(Wildcard.all), "TOTAL")))
				    .from(tableRelationalPath).where(ExpressionUtils.eq(Expressions.stringPath(docAlias, "uid"), Expressions.constant("test") )).limit(10);
			sqlQuery.setUseLiterals(true);
			
			List result = sqlQuery.fetch();
			System.out.println(result);

			String queryStr = sqlQuery.getSQL().getSQL();
			System.out.println(queryStr);

			Statement stmt0 = con.createStatement();
			ResultSet rs0 = stmt0.executeQuery(queryStr);
			System.out.println(rs0);
			
			System.exit(-1);

			// Get tables
			ResultSet catalogs = con.getMetaData().getTables("gluu", null, null, null);
//			System.out.println(catalogs);
			while (catalogs.next()) {
				System.out.println(catalogs.getString("TABLE_NAME"));
				ResultSet columns = con.getMetaData().getColumns("gluu", null, catalogs.getString("TABLE_NAME"), null);
				while(columns.next())
				{
				    String columnName = columns.getString("COLUMN_NAME");
				    String typeName = columns.getString("TYPE_NAME");

				    System.out.println(columnName + "---" + typeName);				}
			}

			// Get types
			ResultSet types = con.getMetaData().getTypeInfo();
			while(types.next())
			{
			    String typeName = types.getString("TYPE_NAME");
			    String dataType = types.getString("DATA_TYPE");
			    
			    System.out.println(typeName + "---" + dataType);
			}

			// Get all clients with types
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select clnt.* from client as clnt");
			
			System.out.println(rs.getMetaData().getColumnCount());
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				System.out.println(rs.getMetaData().getColumnName(i));
				System.out.println(rs.getMetaData().getColumnTypeName(i));
				System.out.println("----------------");
			}
			while (rs.next()) {
				System.out.println(rs.getString(1) + "  " + rs.getString(2) + "  " + rs.getString(3));
			}

			PreparedStatement prep = con.prepareStatement("insert into client (uuid, dn, oxAuthGrantType) VALUES(?, ?, ?)");
			prep.setString(1, UUID.randomUUID().toString());
			prep.setString(2, "dn");
			prep.setString(3, "[\"authorization_code\", \"implicit\", \"refresh_token\"]");
			
			prep.execute();
			
//			con.
			con.close();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	private static DataSource setupDataSource() {
		String connectURI = "jdbc:mysql://localhost:3306/gluu";
		Properties prop = new Properties();
		prop.setProperty("user", "root");
		prop.setProperty("password", "Secret1!");
		prop.setProperty("serverTimezone", "GMT+2");

		ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(connectURI, prop);

		PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
		GenericObjectPoolConfig<PoolableConnection> objectPoolConfig = new GenericObjectPoolConfig<>();
		ObjectPool<PoolableConnection> objectPool = new GenericObjectPool<>(poolableConnectionFactory, objectPoolConfig);
		poolableConnectionFactory.setPool(objectPool);

		PoolingDataSource<PoolableConnection> dataSource = new PoolingDataSource<>(objectPool);
		
		return dataSource;
	}
}
