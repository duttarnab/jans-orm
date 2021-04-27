/*
 * Janssen Project software is available under the MIT License (2008). See http://opensource.org/licenses/MIT for full text.
 *
 * Copyright (c) 2020, Janssen Project
 */

package io.jans.orm.cloud.spanner.operation.impl;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;
import com.google.cloud.spanner.Type.StructField;

import io.jans.orm.cloud.spanner.impl.SpannerBatchOperationWraper;
import io.jans.orm.cloud.spanner.model.ConvertedExpression;
import io.jans.orm.cloud.spanner.model.SearchReturnDataType;
import io.jans.orm.cloud.spanner.model.TableMapping;
import io.jans.orm.cloud.spanner.operation.SpannerOperationService;
import io.jans.orm.cloud.spanner.operation.watch.OperationDurationUtil;
import io.jans.orm.exception.extension.PersistenceExtension;
import io.jans.orm.exception.operation.DeleteException;
import io.jans.orm.exception.operation.DuplicateEntryException;
import io.jans.orm.exception.operation.EntryConvertationException;
import io.jans.orm.exception.operation.EntryNotFoundException;
import io.jans.orm.exception.operation.PersistenceException;
import io.jans.orm.exception.operation.SearchException;
import io.jans.orm.model.AttributeData;
import io.jans.orm.model.AttributeDataModification;
import io.jans.orm.model.AttributeDataModification.AttributeModificationType;
import io.jans.orm.model.BatchOperation;
import io.jans.orm.model.EntryData;
import io.jans.orm.model.PagedResult;
import io.jans.orm.model.SearchScope;
import io.jans.orm.model.Sort;
import io.jans.orm.model.SortOrder;
import io.jans.orm.operation.auth.PasswordEncryptionHelper;
import io.jans.orm.util.ArrayHelper;
import io.jans.orm.util.StringHelper;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * Base service which performs all supported SQL operations
 *
 * @author Yuriy Movchan Date: 12/22/2020
 */
public class SpannerOperationServiceImpl implements SpannerOperationService {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerOperationServiceImpl.class);

	public static final Object[] NO_OBJECTS = new Object[0];

    private Properties props;
    private SpannerConnectionProvider connectionProvider;

	private boolean disableAttributeMapping = false;

	private PersistenceExtension persistenceExtension;

	private DatabaseClient databaseClient;

	private Table tableAlias = new Table("doc");

    @SuppressWarnings("unused")
    private SpannerOperationServiceImpl() {
    }

    public SpannerOperationServiceImpl(Properties props, SpannerConnectionProvider connectionProvider) {
        this.props = props;
        this.connectionProvider = connectionProvider;
        init();
    }

	private void init() {
		this.databaseClient = connectionProvider.getClient();
	}

    @Override
    public SpannerConnectionProvider getConnectionProvider() {
        return connectionProvider;
    }

    @Override
    public boolean authenticate(String key, String password, String objectClass) throws SearchException {
        return authenticateImpl(key, password, objectClass);
    }

    private boolean authenticateImpl(String key, String password, String objectClass) throws SearchException {
        Instant startTime = OperationDurationUtil.instance().now();
        
        boolean result = false;
        if (password != null) {
	        try {
		        List<AttributeData> attributes = lookup(key, objectClass, USER_PASSWORD);
		        
		        Object userPasswordObj = null;
		        for (AttributeData attribute : attributes) {
		        	if (StringHelper.equalsIgnoreCase(attribute.getName(), USER_PASSWORD)) {
		        		userPasswordObj = attribute.getValue();
		        	}
		        	
		        }
		
		        String userPassword = null;
		        if (userPasswordObj instanceof String) {
		            userPassword = (String) userPasswordObj;
		        }
		
		        if (userPassword != null) {
		        	if (persistenceExtension == null) {
			        	result = PasswordEncryptionHelper.compareCredentials(password, userPassword);
		        	} else {
		        		result = persistenceExtension.compareHashedPasswords(password, userPassword);
		        	}
		        }
	        } catch (EntryConvertationException ex) {
	        	throw new SearchException(String.format("Failed to get '%s' attribute", USER_PASSWORD), ex);
	        }
        }

        Duration duration = OperationDurationUtil.instance().duration(startTime);

        TableMapping tableMapping = connectionProvider.getTableMappingByKey(key, objectClass);
        OperationDurationUtil.instance().logDebug("SQL operation: bind, duration: {}, table: {}, key: {}", duration, tableMapping.getTableName(), key);

        return result;
    }

    @Override
    public boolean addEntry(String key, String objectClass, Collection<AttributeData> attributes) throws DuplicateEntryException, PersistenceException {
        Instant startTime = OperationDurationUtil.instance().now();

        TableMapping tableMapping = connectionProvider.getTableMappingByKey(key, objectClass);
        boolean result = addEntryImpl(tableMapping, key, attributes);

        Duration duration = OperationDurationUtil.instance().duration(startTime);
        OperationDurationUtil.instance().logDebug("SQL operation: add, duration: {}, table: {}, key: {}, attributes: {}", duration, tableMapping.getTableName(), key, attributes);
        
        return result;
    }

	private boolean addEntryImpl(TableMapping tableMapping, String key, Collection<AttributeData> attributes) throws PersistenceException {
		try {
			MessageDigest messageDigest = getMessageDigestInstance();
			Map<String, String> columTypes = tableMapping.getColumTypes();

			WriteBuilder mutationBuilder = Mutation.newInsertOrUpdateBuilder(tableMapping.getTableName());
			List<Mutation> mutations = new LinkedList<>();
			for (AttributeData attribute : attributes) {
				String attributeName = attribute.getName();
				String attributeType = columTypes.get(attributeName.toLowerCase());

				// If column not inside table we should check if there is child table
				if (attributeType == null) {
					TableMapping childTableMapping = connectionProvider.getChildTableMappingByKey(key, tableMapping, attributeName);
					if (childTableMapping == null) {
			            throw new PersistenceException(String.format("Failed to add entry. Column '%s' is undefined", attributeName));
					}

					Map<String, String> childColumTypes = childTableMapping.getColumTypes();
					String childAttributeType = childColumTypes.get(attributeName.toLowerCase());
					
					// Build Mutation for child table
					WriteBuilder childMutationBuilder = Mutation.newInsertOrUpdateBuilder(childTableMapping.getTableName());
					for (Object value : attribute.getValues()) {
						// Build Mutation for child table
						String dictDocId = getStringUniqueKey(messageDigest, value);
						childMutationBuilder.
							set(SpannerOperationService.DOC_ID).to(key).
							set(SpannerOperationService.DICT_DOC_ID).to(dictDocId);
						
						setMutationBuilderValue(childMutationBuilder, attribute, childAttributeType, value);

						mutations.add(childMutationBuilder.build());
					}
				} else {
					setMutationBuilderValue(mutationBuilder, attribute, attributeType, attribute.getValues());
				}
			}
			mutations.add(0, mutationBuilder.build());

			databaseClient.write(mutations);

			return true;
        } catch (SpannerException | IllegalStateException ex) {
            throw new PersistenceException("Failed to add entry", ex);
        }
	}

	@Override
    public boolean updateEntry(String key, String objectClass, List<AttributeDataModification> mods) throws UnsupportedOperationException, PersistenceException {
        Instant startTime = OperationDurationUtil.instance().now();
        
        TableMapping tableMapping = connectionProvider.getTableMappingByKey(key, objectClass);
        boolean result = updateEntryImpl(tableMapping, key, mods);

        Duration duration = OperationDurationUtil.instance().duration(startTime);
        OperationDurationUtil.instance().logDebug("SQL operation: modify, duration: {}, table: {}, key: {}, mods: {}", duration, tableMapping.getTableName(), key, mods);

        return result;
    }

	private boolean updateEntryImpl(TableMapping tableMapping, String key, List<AttributeDataModification> mods)
			throws PersistenceException {
		try {
			MessageDigest messageDigest = getMessageDigestInstance();
			Map<String, String> columTypes = tableMapping.getColumTypes();

			WriteBuilder mutationBuilder = Mutation.newInsertOrUpdateBuilder(tableMapping.getTableName());
			List<Mutation> mutations = new LinkedList<>();
			for (AttributeDataModification attributeMod : mods) {
				AttributeData attribute = attributeMod.getAttribute();
				AttributeModificationType type = attributeMod.getModificationType();

				String attributeName = attribute.getName();
				String attributeType = columTypes.get(attributeName.toLowerCase());

				// If column not inside table we should check if there is child table
				if (attributeType == null) {
					TableMapping childTableMapping = connectionProvider.getChildTableMappingByKey(key, tableMapping, attributeName);
					if (childTableMapping == null) {
						throw new PersistenceException(
								String.format("Failed to update entry. Column '%s' is undefined", attributeName));
					}

					Map<String, String> childColumTypes = childTableMapping.getColumTypes();
					String childAttributeType = childColumTypes.get(attributeName.toLowerCase());

					// Build Mutation for child table
					WriteBuilder childMutationBuilder = Mutation.newInsertOrUpdateBuilder(childTableMapping.getTableName());
					for (Object value : attribute.getValues()) {
						String dictDocId = getStringUniqueKey(messageDigest, value);

						if ((AttributeModificationType.ADD == type) ||
							(AttributeModificationType.FORCE_UPDATE == type) || (AttributeModificationType.REPLACE == type)) {

							childMutationBuilder.
								set(SpannerOperationService.DOC_ID).to(key).
								set(SpannerOperationService.DICT_DOC_ID).to(dictDocId);

							setMutationBuilderValue(childMutationBuilder, attribute, childAttributeType, value);

							mutations.add(childMutationBuilder.build());
						} else if (AttributeModificationType.REMOVE == type) {
							// Build Mutation for child table
							Mutation childMutation = Mutation.delete(childTableMapping.getTableName(), Key.of(key, dictDocId));
							mutations.add(childMutation);
						} else {
							throw new UnsupportedOperationException("Operation type '" + type + "' is not implemented");
						}

					}
				} else {
					if ((AttributeModificationType.ADD == type) || (AttributeModificationType.FORCE_UPDATE == type)
							|| (AttributeModificationType.REPLACE == type)) {
						setMutationBuilderValue(mutationBuilder, attribute, attributeType, attribute.getValues());
					} else if (AttributeModificationType.REMOVE == type) {
						removeMutationBuilderValue(mutationBuilder, attribute, attributeType);
					} else {
						throw new UnsupportedOperationException("Operation type '" + type + "' is not implemented");
					}

				}
			}
			mutations.add(0, mutationBuilder.build());

			databaseClient.write(mutations);

			return true;
		} catch (SpannerException | IllegalStateException ex) {
			throw new PersistenceException("Failed to update entry", ex);
		}
	}

	@Override
    public boolean delete(String key, String objectClass) throws EntryNotFoundException {
        Instant startTime = OperationDurationUtil.instance().now();

        TableMapping tableMapping = connectionProvider.getTableMappingByKey(key, objectClass);
        boolean result = deleteImpl(tableMapping, key);

        Duration duration = OperationDurationUtil.instance().duration(startTime);
        OperationDurationUtil.instance().logDebug("SQL operation: delete, duration: {}, table: {}, key: {}", duration, tableMapping.getTableName(), key);

        return result;
    }

	private boolean deleteImpl(TableMapping tableMapping, String key) throws EntryNotFoundException {
		try {
			List<Mutation> mutations = new ArrayList<>();

			mutations.add(
				Mutation.delete(tableMapping.getTableName(), Key.of(key))
			);
			databaseClient.write(mutations);

			return true;
        } catch (SpannerException ex) {
            throw new EntryNotFoundException("Failed to delete entry", ex);
        }
	}

    @Override
    public long delete(String key, String objectClass, ConvertedExpression expression, int count) throws DeleteException {
        Instant startTime = OperationDurationUtil.instance().now();

        TableMapping tableMapping = connectionProvider.getTableMappingByKey(key, objectClass);

    	long result = deleteImpl(tableMapping, expression, count);

        Duration duration = OperationDurationUtil.instance().duration(startTime);
        OperationDurationUtil.instance().logDebug("SQL operation: delete_search, duration: {}, table: {}, key: {}, expression: {}, count: {}", duration, tableMapping.getTableName(), key, expression, count);

        return result;
    }

    private long deleteImpl(TableMapping tableMapping, ConvertedExpression expression, int count) throws DeleteException {
		try {
			Table table = buildTable(tableMapping);

			Delete sqlDeleteQuery = new Delete();
			sqlDeleteQuery.setTable(table);
			sqlDeleteQuery.setWhere(expression.expression());

			if (count > 0) {
				Limit limit = new Limit();
				limit.setRowCount(new LongValue(count));
				sqlDeleteQuery.setLimit(limit);
            }

			Statement statement = Statement.of(sqlDeleteQuery.toString());
            LOG.debug("Executing delete query: '{}'", statement);

			long rowDeleted = databaseClient.executePartitionedUpdate(statement);

			return rowDeleted;
        } catch (SpannerException ex) {
            throw new DeleteException(String.format("Failed to delete entries. Expression: '%s'", expression.expression()), ex);
        }
	}

    @Override
    public boolean deleteRecursively(String key, String objectClass) throws EntryNotFoundException, SearchException {
        Instant startTime = OperationDurationUtil.instance().now();

        TableMapping tableMapping = connectionProvider.getTableMappingByKey(key, objectClass);
        boolean result = deleteRecursivelyImpl(tableMapping, key);

        Duration duration = OperationDurationUtil.instance().duration(startTime);
        OperationDurationUtil.instance().logDebug("SQL operation: delete_tree, duration: {}, table: {}, key: {}", duration, tableMapping.getTableName(), key);

        return result;
    }

	private boolean deleteRecursivelyImpl(TableMapping tableMapping, String key) throws SearchException, EntryNotFoundException {
    	LOG.warn("Removing only base key without sub-tree. Table: {}, Key: {}", tableMapping.getTableName(), key);
    	return deleteImpl(tableMapping, key);
	}

    @Override
    public List<AttributeData> lookup(String key, String objectClass, String... attributes) throws SearchException, EntryConvertationException {
        Instant startTime = OperationDurationUtil.instance().now();
        
    	TableMapping tableMapping = connectionProvider.getTableMappingByKey(key, objectClass);

    	List<AttributeData> result = lookupImpl(tableMapping, key, attributes);

        Duration duration = OperationDurationUtil.instance().duration(startTime);
        OperationDurationUtil.instance().logDebug("SQL operation: lookup, duration: {}, table: {}, key: {}, attributes: {}", duration, tableMapping.getTableName(), key, attributes);

        return result;
    }

	private List<AttributeData> lookupImpl(TableMapping tableMapping, String key, String... attributes) throws SearchException, EntryConvertationException {
		try {
			String tableName = tableMapping.getTableName();

			// If all requested attributes belong to one table get row by primary key
			Set<String> childTables = connectionProvider.getChildTables(tableName);
			List<AttributeData> result = null;
			if (childTables == null) {
				// All attributes in one table
				if (attributes == null) {
					// Request all attributes
	                try (ResultSet resultSet = databaseClient.singleUse().read(tableName, KeySet.singleKey(Key.of(key)), tableMapping.getColumTypes().keySet())) {
	    				result = getAttributeDataList(tableMapping.getObjectClass(), resultSet, true);
	                }
				} else {
					// Request only required attributes
	                try (ResultSet resultSet = databaseClient.singleUse().read(tableName, KeySet.singleKey(Key.of(key)), Arrays.asList(attributes))) {
	    				result = getAttributeDataList(tableMapping.getObjectClass(), resultSet, true);
	                }
				}
			} else {
				Table table = buildTable(tableMapping);
				PlainSelect sqlSelectQuery = new PlainSelect();
				sqlSelectQuery.setFromItem(table);

				List<SelectItem> selectItems = buildSelectAttributes(tableMapping, key, attributes);
				sqlSelectQuery.addSelectItems(selectItems);

				Column leftColumn = new Column(DOC_ID);
				leftColumn.setTable(tableAlias);
				StringValue rightValue = new StringValue(DOC_ID_BINDING);

				EqualsTo whereExp = new EqualsTo(leftColumn, rightValue);
				sqlSelectQuery.setWhere(whereExp);

				Limit limit = new Limit();
				limit.setRowCount(new LongValue(1));
	    		sqlSelectQuery.setLimit(limit);

	    		Statement statement = Statement.newBuilder(sqlSelectQuery.toString()).bind(DOC_ID).to(key).build();
                LOG.debug("Executing lookup query: '{}'", statement);

                try (ResultSet resultSet = databaseClient.singleUse().executeQuery(statement)) {
    				result = getAttributeDataList(tableMapping.getObjectClass(), resultSet, true);
                }
			}

			if (result != null) {
				return result;
			}
		} catch (SpannerException ex) {
			throw new SearchException(String.format("Failed to lookup query by key: '%s'", key), ex);
		}

		throw new SearchException(String.format("Failed to lookup entry by key: '%s'", key));
	}

	@Override
    public <O> PagedResult<EntryData> search(String key, String objectClass, ConvertedExpression expression, SearchScope scope, String[] attributes, Sort[] orderBy,
                                              SpannerBatchOperationWraper<O> batchOperationWraper, SearchReturnDataType returnDataType, int start, int count, int pageSize) throws SearchException {
        Instant startTime = OperationDurationUtil.instance().now();

        TableMapping tableMapping = connectionProvider.getTableMappingByKey(key, objectClass);

        PagedResult<EntryData> result = searchImpl(tableMapping, key, expression, scope, attributes, orderBy, batchOperationWraper,
						returnDataType, start, count, pageSize);

        Duration duration = OperationDurationUtil.instance().duration(startTime);
        OperationDurationUtil.instance().logDebug("SQL operation: search, duration: {}, table: {}, key: {}, expression: {}, scope: {}, attributes: {}, orderBy: {}, batchOperationWraper: {}, returnDataType: {}, start: {}, count: {}, pageSize: {}", duration, tableMapping.getTableName(), key, expression, scope, attributes, orderBy, batchOperationWraper, returnDataType, start, count, pageSize);

        return result;
	}

	private <O> PagedResult<EntryData> searchImpl(TableMapping tableMapping, String key, ConvertedExpression expression, SearchScope scope, String[] attributes, Sort[] orderBy,
            SpannerBatchOperationWraper<O> batchOperationWraper, SearchReturnDataType returnDataType, int start, int count, int pageSize) throws SearchException {
        BatchOperation<O> batchOperation = null;
        if (batchOperationWraper != null) {
            batchOperation = (BatchOperation<O>) batchOperationWraper.getBatchOperation();
        }

		Table table = buildTable(tableMapping);

		PlainSelect sqlSelectQuery = new PlainSelect();
		sqlSelectQuery.setFromItem(table);

		List<SelectItem> selectItems = buildSelectAttributes(tableMapping, key, attributes);
		sqlSelectQuery.addSelectItems(selectItems);
		
		if (expression != null) {
			Expression whereExp = expression.expression();
			sqlSelectQuery.setWhere(whereExp);
		}

        if (orderBy != null) {
        	OrderByElement[] orderByElements = new OrderByElement[orderBy.length];
        	for (int i = 0; i < orderBy.length; i++) {
        		Column column = new Column(orderBy[i].getName());
        		orderByElements[i] = new OrderByElement();
        		orderByElements[i].setExpression(column);
        		
        		if (orderBy[i].getSortOrder() != null) {
        			orderByElements[i].setAscDescPresent(true);
        			orderByElements[i].setAsc(SortOrder.ASCENDING == orderBy[i].getSortOrder());
        		}
        	}

            sqlSelectQuery.withOrderByElements(Arrays.asList(orderByElements));
        }

        List<EntryData> searchResultList = new LinkedList<EntryData>();
        if ((SearchReturnDataType.SEARCH == returnDataType) || (SearchReturnDataType.SEARCH_COUNT == returnDataType)) {
        	List<EntryData> lastResult = null;
	        if (pageSize > 0) {
	    		boolean collectSearchResult;
	    		Limit limit = new Limit();
	    		sqlSelectQuery.setLimit(limit);
	    		
	    		Offset offset = new Offset();
	    		sqlSelectQuery.setOffset(offset);
	
	            int currentLimit;
	    		try {
	                int resultCount = 0;
	                int lastCountRows = 0;
	                do {
	                    collectSearchResult = true;
	
	                    currentLimit = pageSize;
	                    if (count > 0) {
	                        currentLimit = Math.min(pageSize, count - resultCount);
	                    }

	                    // Change limit and offset
	    	    		limit.setRowCount(new LongValue(currentLimit));
	    	    		offset.setOffset(start + resultCount);
	                    
	                    Statement statement = Statement.of(sqlSelectQuery.toString());
	                    LOG.debug("Executing query: '{}'", statement);

	                    try (ResultSet resultSet = databaseClient.singleUse().executeQuery(statement)) {
	                    	lastResult = getEntryDataList(tableMapping.getObjectClass(), resultSet);
	                    }

		    			lastCountRows = lastResult.size();
		    			
	                    if (batchOperation != null) {
	                        collectSearchResult = batchOperation.collectSearchResult(lastCountRows);
	                    }
	                    if (collectSearchResult) {
	                        searchResultList.addAll(lastResult);
	                    }
	
	                    if (batchOperation != null) {
	                        List<O> entries = batchOperationWraper.createEntities(lastResult);
	                        batchOperation.performAction(entries);
	                    }
	
	                    resultCount += lastCountRows;
	
	                    if ((count > 0) && (resultCount >= count)) {
	                        break;
	                    }
	                } while (lastCountRows > 0);
	    		} catch (SpannerException | EntryConvertationException ex) {
	    			throw new SearchException(String.format("Failed to execute query '%s'  with key: '%s'", sqlSelectQuery, key), ex);
	    		}
	        } else {
	    		try {
	                if (count > 0) {
	    	    		Limit limit = new Limit();
	    	    		limit.setRowCount(new LongValue(count));
	    	    		sqlSelectQuery.setLimit(limit);
	                }
	                if (start > 0) {
	    	    		Offset offset = new Offset();
	    	    		offset.setOffset(start);
	    	    		sqlSelectQuery.setOffset(offset);
	                }
	
                    Statement statement = Statement.of(sqlSelectQuery.toString());
                    LOG.debug("Executing query: '{}'", statement);

                    try (ResultSet resultSet = databaseClient.singleUse().executeQuery(statement)) {
		    			lastResult = getEntryDataList(tableMapping.getObjectClass(), resultSet);
		    			searchResultList.addAll(lastResult);
                    }
	            } catch (SpannerException | EntryConvertationException ex) {
	                throw new SearchException(String.format("Failed to execute query '%s'  with key: '%s'", sqlSelectQuery, key), ex);
	            }
	        }
        }

        PagedResult<EntryData> result = new PagedResult<EntryData>();
        result.setEntries(searchResultList);
        result.setEntriesCount(searchResultList.size());
        result.setStart(start);

        if ((SearchReturnDataType.COUNT == returnDataType) || (SearchReturnDataType.SEARCH_COUNT == returnDataType)) {
    		PlainSelect sqlCountSelectQuery = new PlainSelect();
    		sqlCountSelectQuery.setFromItem(table);

    		Function countFunction = new Function();
    		countFunction.setName("COUNT");
    		countFunction.setAllColumns(true);

    		SelectExpressionItem selectCountItem = new SelectExpressionItem(countFunction);
    		selectCountItem.setAlias(new Alias("TOTAL", false));

    		sqlCountSelectQuery.addSelectItems(selectCountItem);
    		
    		if (expression != null) {
    			Expression whereExp = expression.expression();
    			sqlCountSelectQuery.setWhere(whereExp);
    		}

    		try {
                Statement statement = Statement.of(sqlCountSelectQuery.toString());
                LOG.debug("Calculating count. Executing query: '{}'", statement);

                try (ResultSet countResult = databaseClient.singleUse().executeQuery(statement)) {
                	if (!countResult.next()) {
                        throw new SearchException(String.format("Failed to calculate count entries. Query: '%s'", statement));
                	}

                	result.setTotalEntriesCount((int) countResult.getLong("TOTAL"));
                }
    		} catch (SpannerException ex) {
    			throw new SearchException(String.format("Failed to build count search entries query. Key: '%s', expression: '%s'", key, expression.expression()), ex);
            }
        }

        return result;
    }

	public String[] createStoragePassword(String[] passwords) {
        if (ArrayHelper.isEmpty(passwords)) {
            return passwords;
        }

        String[] results = new String[passwords.length];
        for (int i = 0; i < passwords.length; i++) {
			if (persistenceExtension == null) {
				results[i] = PasswordEncryptionHelper.createStoragePassword(passwords[i], connectionProvider.getPasswordEncryptionMethod());
			} else {
				results[i] = persistenceExtension.createHashedPassword(passwords[i]);
			}
        }

        return results;
    }

    private List<AttributeData> getAttributeDataList(String objectClass, ResultSet resultSet, boolean skipDn) throws EntryConvertationException {
        try {
            if ((resultSet == null)) {
                return null;
            }

            if (!resultSet.next()) {
            	return null;
            }

            List<AttributeData> result = new ArrayList<AttributeData>();

            Set<String> nullableColumns = connectionProvider.getTableNullableColumns(objectClass);

	        List<StructField> structFields = resultSet.getType().getStructFields();
	        int columnsCount = resultSet.getColumnCount();
	        for (int i = 0; i < columnsCount; i++) {
	        	StructField structField = structFields.get(i);
	        	String attributeName = structField.getName();
	        	Code columnTypeCode = structField.getType().getCode();
	        	boolean isNullable = nullableColumns.contains(attributeName.toLowerCase());

	        	if (SpannerOperationService.DOC_ID.equalsIgnoreCase(attributeName) ||
	        		SpannerOperationService.ID.equalsIgnoreCase(attributeName)) {
	        		// Skip internal attributes 
	        		continue;
	        	}

	        	if (skipDn && SpannerOperationService.DN.equalsIgnoreCase(attributeName)) {
	        		// Skip DN attribute 
	        		continue;
	        	}
	
	        	Boolean multiValued = Boolean.FALSE;
	            Object[] attributeValueObjects;
	            if (resultSet.isNull(i)) {
	                attributeValueObjects = NO_OBJECTS;
	                if (isNullable) {
	                	// Ignore columns with default NULL values
	                	continue;
	                }
	            } else {
	            	if (Code.ARRAY == columnTypeCode) {
	            		attributeValueObjects = convertDbArrayToValue(resultSet, structField.getType().getArrayElementType(), i, attributeName);
	            		multiValued = Boolean.TRUE;
	            	} else if (Code.BOOL == columnTypeCode) {
	            		attributeValueObjects = new Object[] { resultSet.getBoolean(i) };
	            	} else if (Code.DATE == columnTypeCode) {
	            		attributeValueObjects = new Object[] { com.google.cloud.Date.toJavaUtilDate(resultSet.getDate(i)) };
	            	} else if (Code.TIMESTAMP == columnTypeCode) {
	            		attributeValueObjects = new Object[] { resultSet.getTimestamp(i).toSqlTimestamp().getTime() };
	            	} else if (Code.INT64 == columnTypeCode) {
	            		attributeValueObjects = new Object[] { resultSet.getLong(i) };
	            	} else if (Code.NUMERIC == columnTypeCode) {
	            		attributeValueObjects = new Object[] { resultSet.getBigDecimal(i).longValue() };
	            	} else if (Code.STRING == columnTypeCode) {
						Object value = resultSet.getString(i);
						try {
							SimpleDateFormat jsonDateFormat = new SimpleDateFormat(SQL_DATA_FORMAT);
							value = jsonDateFormat.parse(value.toString());
						} catch (Exception ex) {
						}
						attributeValueObjects = new Object[] { value };
					} else {
						throw new EntryConvertationException(
								String.format("Column with name '%s' does not contain unsupported type '%s'", attributeName, columnTypeCode));
					}
	            }
	            
	            unescapeValues(attributeValueObjects);
	
	            AttributeData tmpAttribute = new AttributeData(attributeName, attributeValueObjects, multiValued);
	            if (multiValued != null) {
	            	tmpAttribute.setMultiValued(multiValued);
	            }
	            result.add(tmpAttribute);
	        }

	        return result;
        } catch (SpannerException ex) {
        	throw new EntryConvertationException("Failed to convert entry!", ex);
        }
    }

    private List<EntryData> getEntryDataList(String objectClass, ResultSet resultSet) throws EntryConvertationException {
    	List<EntryData> entryDataList = new LinkedList<>();

    	List<AttributeData> attributeDataList = null;
    	do  {
	        int columnsCount = resultSet.getColumnCount();
	        Type[] columnTypes = new Type[columnsCount];
	        for (int i = 1; i <= columnsCount; i++) {
	        	columnTypes[i] = resultSet.getColumnType(i);
	        }

    		attributeDataList = getAttributeDataList(objectClass, resultSet, false);
    		if (attributeDataList != null) {
        		EntryData entryData = new EntryData(attributeDataList);
        		entryDataList.add(entryData);
    		}
    	} while (attributeDataList != null);

    	return entryDataList;
	}
      
    @Override
    public boolean isBinaryAttribute(String attribute) {
        return this.connectionProvider.isBinaryAttribute(attribute);
    }

    @Override
    public boolean isCertificateAttribute(String attribute) {
        return this.connectionProvider.isCertificateAttribute(attribute);
    }

    public boolean isDisableAttributeMapping() {
		return disableAttributeMapping;
	}

	@Override
    public boolean destroy() {
        boolean result = true;

        if (connectionProvider != null) {
            try {
                connectionProvider.destroy();
            } catch (Exception ex) {
                LOG.error("Failed to destroy provider correctly");
                result = false;
            }
        }

        return result;
    }

    @Override
    public boolean isConnected() {
        return connectionProvider.isConnected();
    }

    @Override
    public DatabaseClient getConnection() {
        return connectionProvider.getClient();
    }

    @Override
    public Map<String, Map<String, String>> getMetadata() {
        return connectionProvider.getTableColumnsMap();
    }

	@Override
	public void setPersistenceExtension(PersistenceExtension persistenceExtension) {
		this.persistenceExtension = persistenceExtension;
	}

	@Override
	public boolean isSupportObjectClass(String objectClass) {
		return connectionProvider.getTableColumnsMap().containsKey(objectClass);
	}

	private List<SelectItem> buildSelectAttributes(TableMapping tableMapping, String key, String ... attributes) throws SearchException {
		String tableName = tableMapping.getTableName();
		Map<String, String> columTypes = tableMapping.getColumTypes();

		// Table alias for columns
		// Column dn
		Column selectDnColumn = new Column(DN);
		selectDnColumn.setTable(tableAlias);
		SelectExpressionItem selectDnItem = new SelectExpressionItem(selectDnColumn);

		// Column doc_id
		Column selectDocIdColumn = new Column(DOC_ID);
		selectDocIdColumn.setTable(tableAlias);
		SelectExpressionItem selectDocIdItem = new SelectExpressionItem(selectDocIdColumn);

		if (ArrayHelper.isEmpty(attributes)) {
			// Select all columns
			AllTableColumns allColumns = new AllTableColumns(tableAlias);
			List<SelectItem> selectColumns = Arrays.asList(allColumns);

			// Add columns from child tables
			List<SelectExpressionItem> selectChildColumns = buildSelectAttributeFromChildTables(tableName);
			selectColumns.addAll(selectChildColumns);

			return selectColumns;
		} else if ((attributes.length == 1) && StringHelper.isEmpty(attributes[0])) {
        	// Compatibility with base persistence layer when application pass filter new String[] { "" }
			List<SelectItem> selectColumns = Arrays.asList(selectDnItem, selectDocIdItem);

			// Add columns from child tables
			List<SelectExpressionItem> selectChildColumns = buildSelectAttributeFromChildTables(tableName);
			selectColumns.addAll(selectChildColumns);

			return selectColumns;
		}
		
		List<SelectItem> expresisons = new ArrayList<SelectItem>(attributes.length + 2);
		
        boolean hasDn = false;
		for (String attributeName : attributes) {
			String attributeType = columTypes.get(attributeName.toLowerCase());
			SelectExpressionItem selectExpressionItem;

			// If column not inside table we should check if there is child table
			if (attributeType == null) {
				TableMapping childTableMapping = connectionProvider.getChildTableMappingByKey(key, tableMapping, attributeName);
				if (childTableMapping == null) {
		            throw new SearchException(String.format("Failed to build select attributes. Column '%s' is undefined", attributeName));
				}

				// Add columns from child table
				selectExpressionItem = buildSelectAttributeFromChildTable(tableName, attributeName);
			} else {
				Column selectColumn = new Column(attributeName);
				selectColumn.setTable(tableAlias);
				
				selectExpressionItem = new SelectExpressionItem(selectColumn);
			}

			expresisons.add(selectExpressionItem);

			hasDn |= StringHelper.equals(attributeName, DN);
		}

		if (!hasDn) {
			expresisons.add(selectDnItem);
		}

		expresisons.add(selectDocIdItem);

		return expresisons;
	}

	private Table buildTable(TableMapping tableMapping) {
		Table tableRelationalPath = new Table();
		tableRelationalPath.setAlias(new Alias(DOC_ALIAS, false));

		return tableRelationalPath;
	}

	private List<SelectExpressionItem> buildSelectAttributeFromChildTables(String tableName) {
		List<SelectExpressionItem> selectChildColumns = new ArrayList<>();
		Set<String> childAttributes = connectionProvider.getChildTables(tableName);
		if (childAttributes != null) {
			selectChildColumns = new ArrayList<>();
			for (String childAttribute : childAttributes) {
				SelectExpressionItem selectChildColumn = buildSelectAttributeFromChildTable(tableName, childAttribute);
				selectChildColumns.add(selectChildColumn);
			}
		}

		return selectChildColumns;
	}

	private SelectExpressionItem buildSelectAttributeFromChildTable(String tableName, String childAttribute) {
		Function arrayFunction = new Function();
		arrayFunction.setName("ARRAY");
		arrayFunction.setAllColumns(false);

		SelectExpressionItem arraySelectItem = new SelectExpressionItem(arrayFunction);
		arraySelectItem.setAlias(new Alias(childAttribute, false));

		PlainSelect attrSelect = new PlainSelect();

		SubSelect attrSubSelect = new SubSelect();
		attrSubSelect.setSelectBody(attrSelect);
		attrSubSelect.withUseBrackets(false);
		arrayFunction.setParameters(new ExpressionList(attrSubSelect));

		Table attrTableSelect = new Table(tableName + "_" + childAttribute);
		attrTableSelect.setAlias(new Alias("c", false));
		attrSelect.setFromItem(attrTableSelect);
		
		Column attrSelectColumn = new Column(childAttribute);
		attrSelectColumn.setTable(attrTableSelect);

		attrSelect.addSelectItems(new SelectExpressionItem(attrSelectColumn));

		Column attrLeftColumn = new Column(DOC_ID);
		attrLeftColumn.setTable(tableAlias);

		Column attrRightColumn = new Column(DOC_ID);
		attrRightColumn.setTable(attrTableSelect);

		EqualsTo attrEquals = new EqualsTo(attrLeftColumn, attrRightColumn);

		attrSelect.withWhere(attrEquals);

		return arraySelectItem;
	}

	@Override
	public String escapeValue(String value) {
//		return StringHelper.escapeJson(value);
		return value;
	}

	@Override
	public void escapeValues(Object[] realValues) {
//		for (int i = 0; i < realValues.length; i++) {
//        	if (realValues[i] instanceof String) {
//        		realValues[i] = StringHelper.escapeJson(realValues[i]);
//        	}
//        }
	}

	@Override
	public String unescapeValue(String value) {
//		return StringHelper.unescapeJson(value);
		return value;
	}

	@Override
	public void unescapeValues(Object[] realValues) {
//		for (int i = 0; i < realValues.length; i++) {
//        	if (realValues[i] instanceof String) {
//        		realValues[i] = StringHelper.unescapeJson(realValues[i]);
//        	}
//        }
	}

	@Override
	public String toInternalAttribute(String attributeName) {
		return attributeName;
//		if (isDisableAttributeMapping()) {
//			return attributeName;
//		}
//
//		return KeyShortcuter.shortcut(attributeName);
	}

	@Override
	public String[] toInternalAttributes(String[] attributeNames) {
		return attributeNames;
//		if (isDisableAttributeMapping() || ArrayHelper.isEmpty(attributeNames)) {
//			return attributeNames;
//		}
//		
//		String[] resultAttributeNames = new String[attributeNames.length];
//		
//		for (int i = 0; i < attributeNames.length; i++) {
//			resultAttributeNames[i] = KeyShortcuter.shortcut(attributeNames[i]);
//		}
//		
//		return resultAttributeNames;
	}

	@Override
	public String fromInternalAttribute(String internalAttributeName) {
		return internalAttributeName;
//		if (isDisableAttributeMapping()) {
//			return internalAttributeName;
//		}
//
//		return KeyShortcuter.fromShortcut(internalAttributeName);
	}

	@Override
	public String[] fromInternalAttributes(String[] internalAttributeNames) {
		return internalAttributeNames;
//		if (isDisableAttributeMapping() || ArrayHelper.isEmpty(internalAttributeNames)) {
//			return internalAttributeNames;
//		}
//		
//		String[] resultAttributeNames = new String[internalAttributeNames.length];
//		
//		for (int i = 0; i < internalAttributeNames.length; i++) {
//			resultAttributeNames[i] = KeyShortcuter.fromShortcut(internalAttributeNames[i]);
//		}
//		
//		return resultAttributeNames;
	}

	private void setMutationBuilderValue(WriteBuilder mutation, AttributeData attribute, String attributeType,
			Object value) {
		// TODO: Implement
/*
		childMutation.set("").to(value)
		try {
//			String value = JSON_OBJECT_MAPPER.writeValueAsString(propertyValue);

			JsonAttributeValue attributeValue;
			if (propertyValue == null) {
				attributeValue = new JsonAttributeValue();
			} if (propertyValue instanceof List) {
				attributeValue = new JsonAttributeValue(((List) propertyValue).toArray());
			} else if (propertyValue.getClass().isArray()) {
				attributeValue = new JsonAttributeValue((Object[]) propertyValue);
			} else {
				attributeValue = new JsonAttributeValue(new Object[] { propertyValue });
			}

			String value = JSON_OBJECT_MAPPER.writeValueAsString(attributeValue);

			return value;
		} catch (Exception ex) {
			LOG.error("Failed to convert '{}' to json value:", propertyValue, ex);
			throw new MappingException(String.format("Failed to convert '%s' to json value", propertyValue));
		}
*/
	}

	private void removeMutationBuilderValue(WriteBuilder childMutation, AttributeData attribute, String childAttributeType) {
		// TODO: Implement
	}

	private Object[] convertDbArrayToValue(ResultSet resultSet, Type elementType, int columnIndex,
			String attributeName) throws EntryConvertationException {
		Code elementCode = elementType.getCode();
		if (Code.BOOL == elementCode) {
			return resultSet.getBooleanList(columnIndex).toArray(NO_OBJECTS);
		} else if (Code.DATE == elementCode) {
			return toJavaDatesFromSpannerDate(resultSet.getDateList(columnIndex)).toArray(NO_OBJECTS);
		} else if (Code.TIMESTAMP == elementCode) {
			return toJavaDatesFromSpannerTimestamp(resultSet.getTimestampList(columnIndex)).toArray(NO_OBJECTS);
		} else if (Code.INT64 == elementCode) {
			return resultSet.getLongList(columnIndex).toArray(NO_OBJECTS);
		} else if (Code.NUMERIC == elementCode) {
			return toJavaLongFromSpannerNumeric(resultSet.getBigDecimalList(columnIndex)).toArray(NO_OBJECTS);
		} else if (Code.STRING == elementCode) {
			return resultSet.getStringList(columnIndex).toArray(NO_OBJECTS);
		} else {
			throw new EntryConvertationException(String.format(
					"Array column with name '%s' does not contain supported type '%s'", attributeName, elementCode));
		}
	}

	private List<Date> toJavaDatesFromSpannerDate(List<com.google.cloud.Date> dates) {
		List<Date> res = new ArrayList<>(dates.size());
		for (com.google.cloud.Date date : dates) {
			res.add(com.google.cloud.Date.toJavaUtilDate(date));
		}

		return res;
	}

	private List<Date> toJavaDatesFromSpannerTimestamp(List<com.google.cloud.Timestamp> dates) {
		List<Date> res = new ArrayList<>(dates.size());
		for (com.google.cloud.Timestamp date : dates) {
			res.add(new java.util.Date(date.toSqlTimestamp().getTime()));
		}

		return res;
	}

	private List<Long> toJavaLongFromSpannerNumeric(List<BigDecimal> numbers) {
		List<Long> res = new ArrayList<>(numbers.size());
		for (BigDecimal number : numbers) {
			res.add(number.longValue());
		}

		return res;
	}

	public String getStringUniqueKey(MessageDigest messageDigest, Object value) {
		if (value == null) {
			return "null";
		}

		String str = StringHelper.toString(value);
		byte[] digest = messageDigest.digest(str.getBytes(StandardCharsets.UTF_8));

		return Hex.encodeHexString(digest);
	}

	public MessageDigest getMessageDigestInstance() {
		MessageDigest messageDigest;
		try {
			messageDigest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available!");
		}

		return messageDigest;
	}

}
