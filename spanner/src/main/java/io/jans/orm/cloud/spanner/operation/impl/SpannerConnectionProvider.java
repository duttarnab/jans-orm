/*
 * Janssen Project software is available under the MIT License (2008). See http://opensource.org/licenses/MIT for full text.
 *
 * Copyright (c) 2020, Janssen Project
 */

package io.jans.orm.cloud.spanner.operation.impl;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;
import com.google.cloud.spanner.Type.StructField;
import com.google.protobuf.InvalidProtocolBufferException;

import io.jans.orm.cloud.spanner.model.ResultCode;
import io.jans.orm.cloud.spanner.model.TableMapping;
import io.jans.orm.exception.KeyConversionException;
import io.jans.orm.exception.operation.ConfigurationException;
import io.jans.orm.exception.operation.ConnectionException;
import io.jans.orm.operation.auth.PasswordEncryptionMethod;
import io.jans.orm.util.ArrayHelper;
import io.jans.orm.util.PropertiesHelper;
import io.jans.orm.util.StringHelper;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * Perform connection pool initialization
 *
 * @author Yuriy Movchan Date: 04/14/2021
 */
public class SpannerConnectionProvider {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerConnectionProvider.class);
    
    private static final String QUERY_HEALTH_CHECK = "SELECT 1";
    private static final String QUERY_PARENT_TABLE =
    		"SELECT TABLE_NAME, PARENT_TABLE_NAME FROM information_schema.tables WHERE table_catalog = '' and table_schema = '' and parent_table_name is NOT NULL";
    private static final String QUERY_TABLE_SCHEMA =
    		"SELECT TABLE_NAME, COLUMN_NAME, SPANNER_TYPE, IS_NULLABLE FROM information_schema.columns WHERE table_catalog = '' and table_schema = ''";

    private static final String CLIENT_PROPERTIES_PREFIX = "connection.client-property";

    private Properties props;

    private Properties clientConnectionProperties;

    private int creationResultCode;

    private ArrayList<String> binaryAttributes, certificateAttributes;

    private PasswordEncryptionMethod passwordEncryptionMethod;

	private String connectionProject;
	private String connectionInstance;
	private String connectionDatabase;

	private String connectionEmulatorHost;
	
	private String connectionCredentialsFile;
	
	private Map<String, Map<String, StructField>> tableColumnsMap;
	private Map<String, Set<String>> tableNullableColumnsSet;
	private Map<String, Set<String>> childTablesMap;

	private DatabaseClient dbClient;
	private Spanner spanner;

    protected SpannerConnectionProvider() {
    }

    public SpannerConnectionProvider(Properties props) {
        this.props = props;
        this.tableColumnsMap = new HashMap<>();
        this.childTablesMap = new HashMap<>();
    }

    public void create() {
        try {
            init();
        } catch (Exception ex) {
            this.creationResultCode = ResultCode.OPERATIONS_ERROR_INT_VALUE;

            Properties clonedProperties = (Properties) props.clone();

            LOG.error("Failed to create connection with properties: '{}'. Exception: {}", clonedProperties, ex);
        }
    }

    // TODO: Support encryption
	protected void init() throws Exception {
        if (!props.containsKey("connection.project")) {
        	throw new ConfigurationException("Property 'connection.project' is mandatory!");
        }
        this.connectionProject = props.getProperty("connection.project");

        if (!props.containsKey("connection.instance")) {
        	throw new ConfigurationException("Property 'connection.instance' is mandatory!");
        }
        this.connectionInstance = props.getProperty("connection.instance");

        if (!props.containsKey("connection.database")) {
        	throw new ConfigurationException("Property 'connection.database' is mandatory!");
        }
        this.connectionDatabase = props.getProperty("connection.database");

        if (props.containsKey("connection.emulator-host")) {
            this.connectionEmulatorHost = props.getProperty("connection.emulator-host");
        }

		Properties filteredDriverProperties = PropertiesHelper.findProperties(props, CLIENT_PROPERTIES_PREFIX, ".");
        this.clientConnectionProperties = new Properties();
		for (Entry<Object, Object> driverPropertyEntry : filteredDriverProperties.entrySet()) {
			String key = StringHelper.toString(driverPropertyEntry.getKey()).substring(CLIENT_PROPERTIES_PREFIX.length() + 1);
			String value = StringHelper.toString(driverPropertyEntry.getValue());

			clientConnectionProperties.put(key, value);
		}

		this.connectionCredentialsFile = null;
        if (props.containsKey("connection.credentials-file")) {
        	this.connectionCredentialsFile = props.getProperty("connection.credentials-file");
        }

        openWithWaitImpl();
        LOG.info("Created connection pool");

        if (props.containsKey("password.encryption.method")) {
            this.passwordEncryptionMethod = PasswordEncryptionMethod.getMethod(props.getProperty("password.encryption.method"));
        } else {
            this.passwordEncryptionMethod = PasswordEncryptionMethod.HASH_METHOD_SHA256;
        }

        this.binaryAttributes = new ArrayList<String>();
        if (props.containsKey("binaryAttributes")) {
            String[] binaryAttrs = StringHelper.split(props.get("binaryAttributes").toString().toLowerCase(), ",");
            this.binaryAttributes.addAll(Arrays.asList(binaryAttrs));
        }
        LOG.debug("Using next binary attributes: '{}'", binaryAttributes);

        this.certificateAttributes = new ArrayList<String>();
        if (props.containsKey("certificateAttributes")) {
            String[] binaryAttrs = StringHelper.split(props.get("certificateAttributes").toString().toLowerCase(), ",");
            this.certificateAttributes.addAll(Arrays.asList(binaryAttrs));
        }
        LOG.debug("Using next binary certificateAttributes: '{}'", certificateAttributes);

        loadTableMetaData();

        this.creationResultCode = ResultCode.SUCCESS_INT_VALUE;
    }

    private void loadTableMetaData() {
        LOG.info("Scanning DB metadata...");

        long takes = System.currentTimeMillis();
        try (ResultSet resultSet = executeQuery(QUERY_PARENT_TABLE)) {
        	if (resultSet.next()) {
            	int tableNameIdx = resultSet.getColumnIndex("TABLE_NAME");
            	int parentTableNameIdx = resultSet.getColumnIndex("PARENT_TABLE_NAME");
	        	do {
        			String parentTableName = resultSet.getString(parentTableNameIdx);
        			String tableName = resultSet.getString(tableNameIdx);

        			Set<String> childTables;
	        		if (childTablesMap.containsKey(parentTableName)) {
	        			childTables = childTablesMap.get(parentTableName);
	        		} else {
	        			childTables = new HashSet<>();
	        			childTablesMap.put(parentTableName, childTables);
	        		}
	        		
	        		if (tableName.startsWith(parentTableName + "_")) {
	        			tableName = tableName.substring(parentTableName.length() + 1);
	        		}
	        		childTables.add(tableName);
	        	} while (resultSet.next());
        	}
        } catch (SpannerException ex) {
        	throw new ConnectionException("Failed to get database metadata", ex);
        }
        LOG.debug("Build parent tables map: '{}'.", childTablesMap);

        HashMap<String, Type> typeMap = buildSpannerTypesMap();

        try (ResultSet resultSet = executeQuery(QUERY_TABLE_SCHEMA)) {
        	if (resultSet.next()) {
            	int tableNameIdx = resultSet.getColumnIndex("TABLE_NAME");
            	int columnNameIdx = resultSet.getColumnIndex("COLUMN_NAME");
            	int spannerTypeIdx = resultSet.getColumnIndex("SPANNER_TYPE");
            	int isNullableIdx = resultSet.getColumnIndex("IS_NULLABLE");
	        	do {
        			String tableName = resultSet.getString(tableNameIdx);
        			String columnName = resultSet.getString(columnNameIdx);
        			String spannerType = resultSet.getString(spannerTypeIdx);
        			String isNullable = resultSet.getString(isNullableIdx);

	        		// Load table schema
        			Map<String, StructField> tableColumns;
	        		if (tableColumnsMap.containsKey(tableName)) {
	        			tableColumns = tableColumnsMap.get(tableName);
	        		} else {
	            		tableColumns = new HashMap<>();
	                	tableColumnsMap.put(tableName, tableColumns);
	        		}

	        		String comparebleType = toComparableType(spannerType);
	        		Type type = typeMap.get(comparebleType);
	        		if (type == null) {
	                	throw new ConnectionException(String.format("Failed to parse SPANNER_TYPE: '%s'", spannerType));
	        		}
	        		tableColumns.put(columnName.toLowerCase(), StructField.of(columnName, type));

	        		// Check if column nullable
	        		Set<String> nullableColumns;
	        		if (tableNullableColumnsSet.containsKey(tableName)) {
	        			nullableColumns = tableNullableColumnsSet.get(tableName);
	        		} else {
	        			nullableColumns = new HashSet<>();
	        			tableNullableColumnsSet.put(tableName, nullableColumns);
	        		}
	        		
	        		boolean nullable = "yes".equalsIgnoreCase(isNullable);
	        		if (nullable) {
	        			nullableColumns.add(columnName.toLowerCase());
	        		}
	        	} while (resultSet.next());
        	}
        } catch (SpannerException ex) {
        	throw new ConnectionException("Failed to get database metadata", ex);
        }
        LOG.debug("Build table columns map: '{}'.", tableColumnsMap);

        takes = System.currentTimeMillis() - takes;
        LOG.info("Metadata scan finisehd in {} milliseconds", takes);
   	}

	private HashMap<String, Type> buildSpannerTypesMap() {
    	HashMap<String, Type> typeMap = new HashMap<>();
    	
    	// We have to add all types manually because Type is not enum and there is no method to get them all
    	addSpannerType(typeMap, Type.bool());
    	addSpannerType(typeMap, Type.int64());
    	addSpannerType(typeMap, Type.numeric());
    	addSpannerType(typeMap, Type.float64());
    	addSpannerType(typeMap, Type.string());
    	addSpannerType(typeMap, Type.bytes());
    	addSpannerType(typeMap, Type.timestamp());
    	addSpannerType(typeMap, Type.date());

    	return typeMap;
	}

    private String toComparableType(String spannerType) {
    	int idx = spannerType.lastIndexOf("<");
    	if (idx == -1) {
    		return spannerType.toLowerCase();
    	}
    	
    	return spannerType.substring(0, idx).toLowerCase();
	}

	private void addSpannerType(HashMap<String, Type> typeMap, Type type) {
		typeMap.put(type.toString().toLowerCase(), type);
		typeMap.put(Code.ARRAY.name().toLowerCase()  + "<" + type.toString().toLowerCase(), Type.array(type));
	}

	private void openWithWaitImpl() throws Exception {
    	long connectionMaxWaitTimeMillis = StringHelper.toLong(props.getProperty("connection.client.create-max-wait-time-millis"), 30 * 1000L);
        LOG.debug("Using connection timeout: '{}'", connectionMaxWaitTimeMillis);

        Exception lastException = null;

        int attempt = 0;
        long currentTime = System.currentTimeMillis();
        long maxWaitTime = currentTime + connectionMaxWaitTimeMillis;
        do {
            attempt++;
            if (attempt > 0) {
                LOG.info("Attempting to create client connection: '{}'", attempt);
            }

            try {
                open();
                if (isConnected()) {
                	break;
                } else {
                    LOG.info("Failed to connect to Spanner");
                    destroy();
                    throw new ConnectionException("Failed to create client connection");
                }
            } catch (Exception ex) {
                lastException = ex;
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException ex) {
                LOG.error("Exception happened in sleep", ex);
                return;
            }
            currentTime = System.currentTimeMillis();
        } while (maxWaitTime > currentTime);

        if (lastException != null) {
            throw lastException;
        }
    }

    private void open() throws FileNotFoundException, IOException {
        SpannerOptions.Builder optionsBuilder = SpannerOptions.newBuilder();
        if (StringHelper.isNotEmpty(connectionEmulatorHost)) {
        	optionsBuilder.setEmulatorHost(connectionEmulatorHost);
        }

        if (StringHelper.isNotEmpty(connectionCredentialsFile)) {
        	optionsBuilder = optionsBuilder.setCredentials(GoogleCredentials.fromStream(new FileInputStream(connectionCredentialsFile)));
        }

        DatabaseId databaseId = DatabaseId.of(connectionProject, connectionInstance, connectionDatabase);

        this.spanner = optionsBuilder.build().getService();
        this.dbClient = spanner.getDatabaseClient(databaseId);
    }

	public boolean destroy() {
		boolean result = true;
		if (this.spanner != null) {
			try {
				this.spanner.close();
			} catch (RuntimeException ex) {
				LOG.error("Failed to close spanner instance", ex);
				result = false;
			}
		}

		return result;
	}

    public boolean isConnected() {
        if (this.dbClient == null) {
            return false;
        }

        boolean isConnected = true;
        try (ResultSet resultSet = executeQuery(QUERY_HEALTH_CHECK)) {
        	return resultSet.next();
        } catch (Exception ex) {
            LOG.error("Failed to check connection", ex);
            isConnected = false;
        }

        return isConnected;
    }

    public int getCreationResultCode() {
        return creationResultCode;
    }

    public boolean isCreated() {
        return ResultCode.SUCCESS_INT_VALUE == creationResultCode;
    }

    public ArrayList<String> getBinaryAttributes() {
        return binaryAttributes;
    }

    public ArrayList<String> getCertificateAttributes() {
        return certificateAttributes;
    }

    public boolean isBinaryAttribute(String attributeName) {
        if (StringHelper.isEmpty(attributeName)) {
            return false;
        }

        return binaryAttributes.contains(attributeName.toLowerCase());
    }

    public boolean isCertificateAttribute(String attributeName) {
        if (StringHelper.isEmpty(attributeName)) {
            return false;
        }

        return certificateAttributes.contains(attributeName.toLowerCase());
    }

    public PasswordEncryptionMethod getPasswordEncryptionMethod() {
        return passwordEncryptionMethod;
    }

	public TableMapping getTableMappingByKey(String key, String objectClass) {
		String tableName = objectClass;
		Map<String, StructField> columTypes = tableColumnsMap.get(tableName);
		if ("_".equals(key)) {
			return new TableMapping("", tableName, objectClass, columTypes);
		}

		String[] baseNameParts = key.split("_");
		if (ArrayHelper.isEmpty(baseNameParts)) {
			throw new KeyConversionException("Failed to determine base key part!");
		}

		TableMapping tableMapping = new TableMapping(baseNameParts[0], tableName, objectClass, columTypes);
		
		return tableMapping;
	}

	 public TableMapping getChildTableMappingByKey(String key, TableMapping tableMapping, String columnName) {
		String childTableName = tableMapping.getTableName() + "_" + columnName;
		return getTableMappingByKey(key, childTableName);
	}

	public Set<String> getChildTables(String objectClass) {
		return childTablesMap.get(objectClass);
	}

	public Map<String, TableMapping> getChildTablesMapping(String key, TableMapping tableMapping) {
		Set<String> childTableNames = childTablesMap.get(tableMapping.getObjectClass());
		if (childTableNames == null) {
			return null;
		}
		
		Map<String, TableMapping> childTableMapping = new HashMap<>();
		for (String childTableName : childTableNames) {
			TableMapping childColumTypes = getTableMappingByKey(key, childTableName);
			childTableMapping.put(childTableName, childColumTypes);
		}
		
		return childTableMapping;
	}

	public Set<String> getTableNullableColumns(String objectClass) {
		return tableNullableColumnsSet.get(objectClass);
	}

	public DatabaseClient getClient() {
		return dbClient;
	}

	private ResultSet executeQuery(String sql) {
		return this.dbClient.singleUse().executeQuery(Statement.of(sql));
	}

	public Map<String, Map<String, StructField>> getDatabaseMetaData() {
		return tableColumnsMap;
	}

	public static void main(String[] args) throws JSQLParserException, InvalidProtocolBufferException {
		Select selectCount = (Select) CCJSqlParserUtil.parse("SELECT doc.* from doc JOIN jansClnt_Interleave_jansRedirectURI jansRedirectURI ON doc.doc_id = jansRedirectURI.doc_id");
		System.out.println(selectCount);

		Select select = (Select) CCJSqlParserUtil.parse("SELECT doc.*, ARRAY(SELECT c.jansRedirectURI FROM jansClnt_Interleave_jansRedirectURI c WHERE doc.doc_id = c.doc_id) jansRedirectURI\r\n"
				+ "FROM jansClnt_2 AS doc\r\n"
				+ "ORDER BY doc_id, doc_id2 DESC\r\n");
		System.out.println(select);

		Function arrayFunction = new Function();
		arrayFunction.setName("ARRAY");
		arrayFunction.setAllColumns(false);

		SelectExpressionItem selectArrayItem = new SelectExpressionItem(arrayFunction);
		selectArrayItem.setAlias(new Alias("jansRedirectURI", false));

		SubSelect attrSubSelect = new SubSelect();
		PlainSelect attrSelect = new PlainSelect();
		attrSubSelect.setSelectBody(attrSelect);
		attrSubSelect.withUseBrackets(false);
		arrayFunction.setParameters(new ExpressionList(attrSubSelect));

		Table tableAttrSelect = new Table("jansClnt_Interleave_jansRedirectURI");
		tableAttrSelect.setAlias(new Alias("c", false));
		attrSelect.setFromItem(tableAttrSelect);
		
		Column attrSelectColumn = new Column("jansRedirectURI");
		attrSelectColumn.setTable(tableAttrSelect);
		
		attrSelect.addSelectItems(new SelectExpressionItem(attrSelectColumn));

		Column attrLeftColumn = new Column("doc_id");
		attrLeftColumn.setTable(new Table("doc"));

		Column attrRightColumn = new Column("doc_id");
		attrRightColumn.setTable(tableAttrSelect);

		EqualsTo attrEquals = new EqualsTo(attrLeftColumn, attrRightColumn);

		attrSelect.withWhere(attrEquals);

//		selectArrayItem.setExpression(attrSelect);

		System.out.println(selectArrayItem);

		PlainSelect select2 = new PlainSelect();
		Table table = new Table("jansClnt_2");
		table.setAlias(new Alias("doc"));
		select2.setFromItem(table);

		Table tableAlias = new Table("doc");
		
		
		AllTableColumns all = new AllTableColumns(tableAlias);

		Table selectJoinTable = new Table("jansRedirectURI");
		Column selectJoinColumn = new Column("jansRedirectURI");
		selectJoinColumn.setTable(selectJoinTable);
		SelectExpressionItem selectItem = new SelectExpressionItem();
		selectItem.setExpression(selectJoinColumn);
		
		select2.addSelectItems(all, selectItem);

		Table joinTable = new Table("jansClnt_2_jansRedirectURI");
		joinTable.setAlias(new Alias("jansRedirectURI"));
		Table jointTableAlias = new Table("jansRedirectURI");

		Join join = new Join();
		join.setRightItem(joinTable);
		
		Column leftColumn = new Column("doc_id");
		leftColumn.setTable(tableAlias);

		Column rightColumn = new Column("doc_id");
		rightColumn.setTable(jointTableAlias);

		EqualsTo equals = new EqualsTo(leftColumn, rightColumn);
		join.setOnExpression(equals);

		join.setRightItem(joinTable);
		select2.addJoins(join);
		
		Column leftWhereColumn = new Column("jansRedirectURI");
		leftWhereColumn.setTable(jointTableAlias);
		
		StringValue rightPart = new StringValue("https://jenkins-mysql.jans.io/identity/scim/auth");

		EqualsTo where = new EqualsTo(leftWhereColumn, rightPart);
		select2.setWhere(where);
		System.out.println(select2);
		
		Limit limit = new Limit();
		limit.setRowCount(new LongValue(1000));
		select2.setLimit(limit);
		
		Offset offset = new Offset();
		offset.setOffset(3000);
		select2.setOffset(offset);

		System.out.println(select2);

		System.out.println(select.equals(select2));
	}

}
