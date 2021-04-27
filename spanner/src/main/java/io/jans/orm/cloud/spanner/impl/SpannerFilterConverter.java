/*
 * Janssen Project software is available under the MIT License (2008). See http://opensource.org/licenses/MIT for full text.
 *
 * Copyright (c) 2020, Janssen Project
 */

package io.jans.orm.cloud.spanner.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.jans.orm.annotation.AttributeEnum;
import io.jans.orm.cloud.spanner.model.ConvertedExpression;
import io.jans.orm.cloud.spanner.operation.SpannerOperationService;
import io.jans.orm.exception.operation.SearchException;
import io.jans.orm.ldap.impl.LdapFilterConverter;
import io.jans.orm.reflect.property.PropertyAnnotation;
import io.jans.orm.reflect.util.ReflectHelper;
import io.jans.orm.search.filter.Filter;
import io.jans.orm.util.ArrayHelper;
import io.jans.orm.util.StringHelper;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * Filter to Cloud Spanner query convert
 *
 * @author Yuriy Movchan Date: 04/08/2021
 */
public class SpannerFilterConverter {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerFilterConverter.class);
    
    private static final String SPANNER_DATA_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    private static final LdapFilterConverter ldapFilterConverter = new LdapFilterConverter();
	private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();

	private SpannerOperationService operationService;

	private Table stringDocAlias = new Table("doc");
/*
	private Path<Boolean> booleanDocAlias = ExpressionUtils.path(Boolean.class, "doc");
	private Path<Integer> integerDocAlias = ExpressionUtils.path(Integer.class, "doc");
	private Path<Long> longDocAlias = ExpressionUtils.path(Long.class, "doc");
	private Path<Object> objectDocAlias = ExpressionUtils.path(Object.class, "doc");
*/
    public SpannerFilterConverter(SpannerOperationService operationService) {
    	this.operationService = operationService;
	}

	public ConvertedExpression convertToSqlFilter(Filter genericFilter, Map<String, PropertyAnnotation> propertiesAnnotationsMap) throws SearchException {
    	return convertToSqlFilter(genericFilter, propertiesAnnotationsMap, false);
    }

	public ConvertedExpression convertToSqlFilter(Filter genericFilter, Map<String, PropertyAnnotation> propertiesAnnotationsMap, boolean skipAlias) throws SearchException {
    	return convertToSqlFilter(genericFilter, propertiesAnnotationsMap, null, skipAlias);
    }

	public ConvertedExpression convertToSqlFilter(Filter genericFilter, Map<String, PropertyAnnotation> propertiesAnnotationsMap, Function<? super Filter, Boolean> processor) throws SearchException {
    	return convertToSqlFilter(genericFilter, propertiesAnnotationsMap, processor, false);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
	public ConvertedExpression convertToSqlFilter(Filter genericFilter, Map<String, PropertyAnnotation> propertiesAnnotationsMap, Function<? super Filter, Boolean> processor, boolean skipAlias) throws SearchException {
    	Map<String, Class<?>> jsonAttributes = new HashMap<>();
    	ConvertedExpression convertedExpression = convertToSqlFilterImpl(genericFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias);
    	
    	return convertedExpression;
    }

	private ConvertedExpression convertToSqlFilterImpl(Filter genericFilter, Map<String, PropertyAnnotation> propertiesAnnotationsMap,
			Map<String, Class<?>> jsonAttributes, Function<? super Filter, Boolean> processor, boolean skipAlias) throws SearchException {
		if (genericFilter == null) {
			return null;
		}
/*
		Filter currentGenericFilter = genericFilter;

        FilterType type = currentGenericFilter.getType();
        if (FilterType.RAW == type) {
        	LOG.warn("RAW Ldap filter to SQL convertion will be removed in new version!!!");
        	currentGenericFilter = ldapFilterConverter.convertRawLdapFilterToFilter(currentGenericFilter.getFilterString());
        	type = currentGenericFilter.getType();
        }

        if (processor != null) {
        	processor.apply(currentGenericFilter);
        }

        if ((FilterType.NOT == type) || (FilterType.AND == type) || (FilterType.OR == type)) {
            Filter[] genericFilters = currentGenericFilter.getFilters();
            Expression[] expFilters = new Expression[genericFilters.length];

            if (genericFilters != null) {
            	boolean canJoinOrFilters = FilterType.OR == type; // We can replace only multiple OR with IN
            	List<Filter> joinOrFilters = new ArrayList<Filter>();
            	String joinOrAttributeName = null;
                for (int i = 0; i < genericFilters.length; i++) {
                	Filter tmpFilter = genericFilters[i];
                    expFilters[i] = convertToSqlFilterImpl(tmpFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias).expression();

                    // Check if we can replace OR with IN
                	if (!canJoinOrFilters) {
                		continue;
                	}
                	
                	if (tmpFilter.getMultiValued() != null) {
                		canJoinOrFilters = false;
                    	continue;
                	}

                	if ((FilterType.EQUALITY != tmpFilter.getType()) || (tmpFilter.getFilters() != null)) {
                    	canJoinOrFilters = false;
                    	continue;
                    }

                    Boolean isMultiValuedDetected = determineMultiValuedByType(tmpFilter.getAttributeName(), propertiesAnnotationsMap);
                	if (!Boolean.FALSE.equals(isMultiValuedDetected)) {
                		if (!Boolean.FALSE.equals(currentGenericFilter.getMultiValued())) { 
	                		canJoinOrFilters = false;
	                    	continue;
                		}
                	}
                	
            		if (joinOrAttributeName == null) {
            			joinOrAttributeName = tmpFilter.getAttributeName();
            			joinOrFilters.add(tmpFilter);
            			continue;
            		}
            		if (!joinOrAttributeName.equals(tmpFilter.getAttributeName())) {
                		canJoinOrFilters = false;
                    	continue;
            		}
            		joinOrFilters.add(tmpFilter);
                }

                if (FilterType.NOT == type) {
                    return ConvertedExpression.build(new Parenthesis(new NotExpression(expFilters[0])), jsonAttributes);
                } else if (FilterType.AND == type) {
                	Expression result = expFilters[0];
                    for (int i = 1; i < expFilters.length; i++) {
                        result = new AndExpression(result, expFilters[i]);
                    }

                    return ConvertedExpression.build(new Parenthesis(result), jsonAttributes);
                } else if (FilterType.OR == type) {
                    if (canJoinOrFilters) {
                    	List<Expression> rightObjs = new ArrayList<>(joinOrFilters.size());
                    	Filter lastEqFilter = null;
                		for (Filter eqFilter : joinOrFilters) {
                			lastEqFilter = eqFilter;

                			rightObjs.add(buildTypedValueExpression(eqFilter));
            			}
                		
                		return ConvertedExpression.build(new Parenthesis(new InExpression(buildTypedPath(lastEqFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias), new ExpressionList(rightObjs))), jsonAttributes);
                	} else {
                    	Expression result = expFilters[0];
                        for (int i = 1; i < expFilters.length; i++) {
                            result = new OrExpression(result, expFilters[i]);
                        }

                        return ConvertedExpression.build(new Parenthesis(result), jsonAttributes);
                	}
            	}
            }
        }

        if (FilterType.EQUALITY == type) {
    		if (isMultiValue(currentGenericFilter, propertiesAnnotationsMap)) {
    			Expression expression = buildTypedPath(currentGenericFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias);

				Operation<Boolean> operation = ExpressionUtils.predicate(SpannerOps.JSON_CONTAINS, expression,
						buildTypedValueExpression(currentGenericFilter), Expressions.constant("$.v"));

        		return ConvertedExpression.build(operation, jsonAttributes);
            } else {
            	Filter usedFilter = currentGenericFilter;
            	Expression expression = buildTypedPath(currentGenericFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias);

            	return ConvertedExpression.build(new EqualsTo(expression, buildTypedValueExpression(usedFilter)), jsonAttributes);
            }
        }

        if (FilterType.LESS_OR_EQUAL == type) {
        	String internalAttribute = toInternalAttribute(currentGenericFilter);
            if (isMultiValue(currentGenericFilter, propertiesAnnotationsMap)) {
            	if (currentGenericFilter.getMultiValuedCount() > 1) {
                	Collection<Predicate> expressions = new ArrayList<>(currentGenericFilter.getMultiValuedCount());
            		for (int i = 0; i < currentGenericFilter.getMultiValuedCount(); i++) {
                		Operation<Boolean> operation = ExpressionUtils.predicate(SpannerOps.JSON_EXTRACT,
                				buildTypedPath(currentGenericFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias), Expressions.constant("$.v[" + i + "]"));
                		Predicate predicate = Expressions.asComparable(operation).loe(buildTypedValueExpression(currentGenericFilter));

                		expressions.add(predicate);
            		}

            		Expression expression = ExpressionUtils.anyOf(expressions);

            		return ConvertedExpression.build(expression, jsonAttributes);
            	}

            	Operation<Boolean> operation = ExpressionUtils.predicate(SpannerOps.JSON_EXTRACT,
        				buildTypedPath(currentGenericFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias), Expressions.constant("$.v[0]"));
        		Expression expression = Expressions.asComparable(operation).loe(buildTypedValueExpression(currentGenericFilter));

            	return ConvertedExpression.build(expression, jsonAttributes);
            } else {
            	return ConvertedExpression.build(Expressions.asComparable(buildTypedPath(currentGenericFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias)).loe(buildTypedValueExpression(currentGenericFilter)), jsonAttributes);
            }
        }

        if (FilterType.GREATER_OR_EQUAL == type) {
        	String internalAttribute = toInternalAttribute(currentGenericFilter);
            if (isMultiValue(currentGenericFilter, propertiesAnnotationsMap)) {
            	if (currentGenericFilter.getMultiValuedCount() > 1) {
                	Collection<Predicate> expressions = new ArrayList<>(currentGenericFilter.getMultiValuedCount());
            		for (int i = 0; i < currentGenericFilter.getMultiValuedCount(); i++) {
                		Operation<Boolean> operation = ExpressionUtils.predicate(SpannerOps.JSON_EXTRACT,
                				buildTypedPath(currentGenericFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias), Expressions.constant("$.v[" + i + "]"));
                		Predicate predicate = Expressions.asComparable(operation).goe(buildTypedValueExpression(currentGenericFilter));

                		expressions.add(predicate);
            		}
            		Expression expression = ExpressionUtils.anyOf(expressions);

            		return ConvertedExpression.build(expression, jsonAttributes);
            	}

            	Operation<Boolean> operation = ExpressionUtils.predicate(SpannerOps.JSON_EXTRACT,
        				buildTypedPath(currentGenericFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias), Expressions.constant("$.v[0]"));
        		Expression expression = Expressions.asComparable(operation).goe(buildTypedValueExpression(currentGenericFilter));

            	return ConvertedExpression.build(expression, jsonAttributes);
            } else {
            	return ConvertedExpression.build(Expressions.asComparable(buildTypedPath(currentGenericFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias)).goe(buildTypedValueExpression(currentGenericFilter)), jsonAttributes);
            }
        }

        if (FilterType.PRESENCE == type) {
        	String internalAttribute = toInternalAttribute(currentGenericFilter);
        	Expression expression;
            if (isMultiValue(currentGenericFilter, propertiesAnnotationsMap)) {
            	if (currentGenericFilter.getMultiValuedCount() > 1) {
                	Collection<Predicate> expressions = new ArrayList<>(currentGenericFilter.getMultiValuedCount());
            		for (int i = 0; i < currentGenericFilter.getMultiValuedCount(); i++) {
            			Predicate predicate = ExpressionUtils.isNotNull(ExpressionUtils.predicate(SpannerOps.JSON_EXTRACT,
                				buildTypedPath(currentGenericFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias), Expressions.constant("$.v[" + i + "]")));
            			expressions.add(predicate);
            		}
            		Predicate predicate = ExpressionUtils.anyOf(expressions);

            		return ConvertedExpression.build(predicate, jsonAttributes);
            	}

            	expression = ExpressionUtils.predicate(SpannerOps.JSON_EXTRACT,
        				buildTypedPath(currentGenericFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias), Expressions.constant("$.v[0]"));
            } else {
            	expression = buildTypedPath(currentGenericFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias);
            }

            return ConvertedExpression.build(ExpressionUtils.isNotNull(expression), jsonAttributes);
        }

        if (FilterType.APPROXIMATE_MATCH == type) {
            throw new SearchException("Convertion from APPROXIMATE_MATCH LDAP filter to SQL filter is not implemented");
        }

        if (FilterType.SUBSTRING == type) {
        	StringBuilder like = new StringBuilder();
            if (currentGenericFilter.getSubInitial() != null) {
                like.append(currentGenericFilter.getSubInitial());
            }
            like.append("%");

            String[] subAny = currentGenericFilter.getSubAny();
            if ((subAny != null) && (subAny.length > 0)) {
                for (String any : subAny) {
                    like.append(any);
                    like.append("%");
                }
            }

            if (currentGenericFilter.getSubFinal() != null) {
                like.append(currentGenericFilter.getSubFinal());
            }

            String internalAttribute = toInternalAttribute(currentGenericFilter);

            Expression expression;
            if (isMultiValue(currentGenericFilter, propertiesAnnotationsMap)) {
            	if (currentGenericFilter.getMultiValuedCount() > 1) {
                	Collection<Predicate> expressions = new ArrayList<>(currentGenericFilter.getMultiValuedCount());
            		for (int i = 0; i < currentGenericFilter.getMultiValuedCount(); i++) {
                		Operation<Boolean> operation = ExpressionUtils.predicate(SpannerOps.JSON_EXTRACT,
                				buildTypedPath(currentGenericFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias), Expressions.constant("$.v[" + i + "]"));
                		Predicate predicate = Expressions.booleanOperation(Ops.LIKE, operation, Expressions.constant(like.toString()));

                		expressions.add(predicate);
            		}
            		Predicate predicate = ExpressionUtils.anyOf(expressions);

            		return ConvertedExpression.build(predicate, jsonAttributes);
            	}

            	expression = ExpressionUtils.predicate(SpannerOps.JSON_EXTRACT,
        				buildTypedPath(currentGenericFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias), Expressions.constant("$.v[0]"));
            } else {
            	expression = buildTypedPath(currentGenericFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias);
            }

            return ConvertedExpression.build(Expressions.booleanOperation(Ops.LIKE, expression, Expressions.constant(like.toString())), jsonAttributes);
        }

        if (FilterType.LOWERCASE == type) {
        	return ConvertedExpression.build(ExpressionUtils.toLower(buildTypedPath(currentGenericFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias)), jsonAttributes);
        }
        throw new SearchException(String.format("Unknown filter type '%s'", type));
*/
		return null;
	}

	protected Boolean isMultiValue(Filter currentGenericFilter, Map<String, PropertyAnnotation> propertiesAnnotationsMap) {
		Boolean isMultiValuedDetected = determineMultiValuedByType(currentGenericFilter.getAttributeName(), propertiesAnnotationsMap);
		if (Boolean.TRUE.equals(currentGenericFilter.getMultiValued()) || Boolean.TRUE.equals(isMultiValuedDetected)) {
			return true;
		}

		return false;
	}

	private String toInternalAttribute(Filter filter) {
		String attributeName = filter.getAttributeName();

		if (StringHelper.isEmpty(attributeName)) {
			// Try to find inside sub-filter
			for (Filter subFilter : filter.getFilters()) {
				attributeName = subFilter.getAttributeName();
				if (StringHelper.isNotEmpty(attributeName)) {
					break;
				}
			}
		}

		return toInternalAttribute(attributeName);
	}

	private String toInternalAttribute(String attributeName) {
		if (operationService == null) {
			return attributeName;
		}

		return operationService.toInternalAttribute(attributeName);
	}

	private Expression buildTypedValueExpression(Filter filter) throws SearchException {
		if (Boolean.TRUE.equals(filter.getMultiValued())) {
			Object assertionValue = filter.getAssertionValue();
			if (assertionValue instanceof AttributeEnum) {
				return new StringValue(((AttributeEnum) assertionValue).getValue());
			} else if (assertionValue instanceof Date) {
				java.sql.Date sqlDate = new java.sql.Date(((Date) assertionValue).getTime());
		        return new DateValue(sqlDate);
			}
	
	        return null;
//	        return Expressions.constant(convertValueToJson(Arrays.asList(assertionValue)));
		} else {
			Object assertionValue = filter.getAssertionValue();
			if (assertionValue instanceof AttributeEnum) {
				return new StringValue(((AttributeEnum) assertionValue).getValue());
			} else if (assertionValue instanceof Date) {
				java.sql.Date sqlDate = new java.sql.Date(((Date) assertionValue).getTime());
		        return new DateValue(sqlDate);
			}

			if (assertionValue instanceof Boolean) {
				return new LongValue((Boolean) assertionValue ? 1 : 0);
			} else if (assertionValue instanceof Integer) {
				return new LongValue((Integer) assertionValue);
			} else if (assertionValue instanceof Long) {
				return new LongValue((Long) assertionValue);
			}

			return new StringValue((String) assertionValue);
		}
	}

	private Expression buildTypedPath(Filter genericFilter, Map<String, PropertyAnnotation> propertiesAnnotationsMap,
			Map<String, Class<?>> jsonAttributes, Function<? super Filter, Boolean> processor, boolean skipAlias) throws SearchException {
    	boolean hasSubFilters = ArrayHelper.isNotEmpty(genericFilter.getFilters());

		if (hasSubFilters) {
    		return convertToSqlFilterImpl(genericFilter.getFilters()[0], propertiesAnnotationsMap, jsonAttributes, processor, skipAlias).expression();
		}
		
		String internalAttribute = toInternalAttribute(genericFilter);
		
		return buildColumnExpression(genericFilter, internalAttribute, skipAlias);
	}

	private Expression buildColumnExpression(Filter filter, String attributeName, boolean skipAlias) {
    	if (skipAlias) {
    	    return new Column("attributeName");
    	}

    	return new Column(stringDocAlias, "attributeName");
	}

	private Boolean determineMultiValuedByType(String attributeName, Map<String, PropertyAnnotation> propertiesAnnotationsMap) {
		if ((attributeName == null) || (propertiesAnnotationsMap == null)) {
			return null;
		}

		if (StringHelper.equalsIgnoreCase(attributeName, SpannerEntryManager.OBJECT_CLASS)) {
			return false;
		}

		PropertyAnnotation propertyAnnotation = propertiesAnnotationsMap.get(attributeName);
		if ((propertyAnnotation == null) || (propertyAnnotation.getParameterType() == null)) {
			return null;
		}

		Class<?> parameterType = propertyAnnotation.getParameterType();
		
		boolean isMultiValued = parameterType.equals(Object[].class) || parameterType.equals(String[].class) || ReflectHelper.assignableFrom(parameterType, List.class) || ReflectHelper.assignableFrom(parameterType, AttributeEnum[].class);
		
		return isMultiValued;
	}

	protected String convertValueToJson(Object propertyValue) throws SearchException {
		try {
			String value = JSON_OBJECT_MAPPER.writeValueAsString(propertyValue);

			return value;
		} catch (Exception ex) {
			LOG.error("Failed to convert '{}' to json value:", propertyValue, ex);
			throw new SearchException(String.format("Failed to convert '%s' to json value", propertyValue));
		}
	}

}
