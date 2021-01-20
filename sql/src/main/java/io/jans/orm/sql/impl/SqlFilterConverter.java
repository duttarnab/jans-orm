/*
 * Janssen Project software is available under the MIT License (2008). See http://opensource.org/licenses/MIT for full text.
 *
 * Copyright (c) 2020, Janssen Project
 */

package io.jans.orm.sql.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.querydsl.core.types.Expression;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Ops;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.Expressions;

import io.jans.orm.annotation.AttributeEnum;
import io.jans.orm.annotation.AttributeName;
import io.jans.orm.exception.operation.SearchException;
import io.jans.orm.ldap.impl.LdapFilterConverter;
import io.jans.orm.reflect.property.PropertyAnnotation;
import io.jans.orm.reflect.util.ReflectHelper;
import io.jans.orm.search.filter.Filter;
import io.jans.orm.search.filter.FilterType;
import io.jans.orm.sql.model.ConvertedExpression;
import io.jans.orm.util.ArrayHelper;
import io.jans.orm.util.StringHelper;

/**
 * Filter to SQL query convert
 *
 * @author Yuriy Movchan Date: 12/16/2020
 */
public class SqlFilterConverter {

    private static final Logger LOG = LoggerFactory.getLogger(SqlFilterConverter.class);
    
    private LdapFilterConverter ldapFilterConverter = new LdapFilterConverter();

	private SqlEntryManager persistenceEntryManager;

	private Path<String> stringDocAlias = ExpressionUtils.path(String.class, "doc");
	private Path<Boolean> booleanDocAlias = ExpressionUtils.path(Boolean.class, "doc");
	private Path<Integer> integerDocAlias = ExpressionUtils.path(Integer.class, "doc");
	private Path<Long> longDocAlias = ExpressionUtils.path(Long.class, "doc");
	private Path<Object> objectDocAlias = ExpressionUtils.path(Object.class, "doc");

    public SqlFilterConverter(SqlEntryManager persistenceEntryManager) {
    	this.persistenceEntryManager = persistenceEntryManager;
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
            Predicate[] expFilters = new Predicate[genericFilters.length];

            if (genericFilters != null) {
            	boolean canJoinOrFilters = FilterType.OR == type; // We can replace only multiple OR with IN
            	List<Filter> joinOrFilters = new ArrayList<Filter>();
            	String joinOrAttributeName = null;
                for (int i = 0; i < genericFilters.length; i++) {
                	Filter tmpFilter = genericFilters[i];
                    expFilters[i] = (Predicate) convertToSqlFilterImpl(tmpFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias).expression();

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
                    return ConvertedExpression.build(ExpressionUtils.predicate(Ops.NOT, expFilters[0]), jsonAttributes);
                } else if (FilterType.AND == type) {
                    return ConvertedExpression.build(ExpressionUtils.allOf(expFilters), jsonAttributes);
                } else if (FilterType.OR == type) {
                    if (canJoinOrFilters) {
                    	List<Object> rightObjs = new ArrayList<>(joinOrFilters.size());
                    	Filter lastEqFilter = null;
                		for (Filter eqFilter : joinOrFilters) {
                			lastEqFilter = eqFilter;
                			rightObjs.add(eqFilter.getAssertionValue());
            			}
                		
                		String internalAttributeName = toInternalAttribute(joinOrAttributeName);
                		return ConvertedExpression.build(ExpressionUtils.in(buildTypedPath(lastEqFilter, internalAttributeName, skipAlias), rightObjs), jsonAttributes);
                	} else {
                        return ConvertedExpression.build(ExpressionUtils.anyOf(expFilters), jsonAttributes);
                	}
            	}
            }
        }

        if (FilterType.EQUALITY == type) {
        	boolean hasSubFilters = ArrayHelper.isNotEmpty(currentGenericFilter.getFilters());
        	Boolean isMultiValuedDetected = determineMultiValuedByType(currentGenericFilter.getAttributeName(), propertiesAnnotationsMap);

        	String internalAttribute = toInternalAttribute(currentGenericFilter);
    		if (Boolean.TRUE.equals(currentGenericFilter.getMultiValued()) || Boolean.TRUE.equals(isMultiValuedDetected)) {
        		if (hasSubFilters) {
            		Filter clonedFilter = currentGenericFilter.getFilters()[0];
            		clonedFilter.setAttributeName(internalAttribute + "_.v$");
            		ConvertedExpression nameConvertedExpression = convertToSqlFilterImpl(clonedFilter, propertiesAnnotationsMap, jsonAttributes, processor, skipAlias);
                	return ConvertedExpression.build(ExpressionUtils.eq(nameConvertedExpression.expression(), buildTypedExpression(currentGenericFilter)), jsonAttributes);
            	}

            	return ConvertedExpression.build(ExpressionUtils.eq(buildTypedPath(currentGenericFilter, internalAttribute + "_.v$", skipAlias), buildTypedExpression(currentGenericFilter)), jsonAttributes);
            } else {
            	if (hasSubFilters) {
            		ConvertedExpression nameExpression = convertToSqlFilterImpl(currentGenericFilter.getFilters()[0], propertiesAnnotationsMap, jsonAttributes, processor, skipAlias);
                    return ConvertedExpression.build(ExpressionUtils.eq(nameExpression.expression(), buildTypedExpression(currentGenericFilter)), jsonAttributes);
            	}

            	return ConvertedExpression.build(ExpressionUtils.eq(buildTypedPath(currentGenericFilter, skipAlias), buildTypedExpression(currentGenericFilter)), jsonAttributes);
            }
        }

        if (FilterType.LESS_OR_EQUAL == type) {
        	String internalAttribute = toInternalAttribute(currentGenericFilter);
            if (isMultiValue(currentGenericFilter, propertiesAnnotationsMap)) {
            	return ConvertedExpression.build(Expressions.asComparable(buildTypedPath(currentGenericFilter, internalAttribute + "_.v$", skipAlias)).loe(buildTypedExpression(currentGenericFilter)), jsonAttributes);
            } else {
            	return ConvertedExpression.build(Expressions.asComparable(buildTypedPath(currentGenericFilter, internalAttribute, skipAlias)).loe(buildTypedExpression(currentGenericFilter)), jsonAttributes);
            }
        }

        if (FilterType.GREATER_OR_EQUAL == type) {
        	String internalAttribute = toInternalAttribute(currentGenericFilter);
            if (isMultiValue(currentGenericFilter, propertiesAnnotationsMap)) {
            	return ConvertedExpression.build(Expressions.asComparable(buildTypedPath(currentGenericFilter, internalAttribute + "_.v$", skipAlias)).goe(buildTypedExpression(currentGenericFilter)), jsonAttributes);
            } else {
            	return ConvertedExpression.build(Expressions.asComparable(buildTypedPath(currentGenericFilter, internalAttribute, skipAlias)).goe(buildTypedExpression(currentGenericFilter)), jsonAttributes);
            }
        }

        if (FilterType.PRESENCE == type) {
        	String internalAttribute = toInternalAttribute(currentGenericFilter);
            if (isMultiValue(currentGenericFilter, propertiesAnnotationsMap)) {
            	return ConvertedExpression.build(ExpressionUtils.isNotNull(buildTypedPath(currentGenericFilter, internalAttribute + "_.v$", skipAlias)), jsonAttributes);
            } else {
            	return ConvertedExpression.build(ExpressionUtils.predicate(Ops.EXISTS, buildTypedPath(currentGenericFilter, internalAttribute, skipAlias)), jsonAttributes);
            }
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
            if (isMultiValue(currentGenericFilter, propertiesAnnotationsMap)) {
            	if (skipAlias) {
                	return ConvertedExpression.build(Expressions.stringPath(internalAttribute + "_.v$").like(Expressions.constant(like.toString())), jsonAttributes);
            	} else {
                	return ConvertedExpression.build(Expressions.stringPath(objectDocAlias, internalAttribute + "_.v$").like(Expressions.constant(like.toString())), jsonAttributes);
            	}
            } else {
            	if (skipAlias) {
                	return ConvertedExpression.build(Expressions.stringPath(internalAttribute).like(Expressions.constant(like.toString())), jsonAttributes);
            	} else {
                	return ConvertedExpression.build(Expressions.stringPath(objectDocAlias, internalAttribute).like(Expressions.constant(like.toString())), jsonAttributes);
            	}
            }
        }

        if (FilterType.LOWERCASE == type) {
        	return ConvertedExpression.build(ExpressionUtils.toLower(buildTypedPath(currentGenericFilter, skipAlias)), jsonAttributes);
        }

        throw new SearchException(String.format("Unknown filter type '%s'", type));
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
		if (persistenceEntryManager == null) {
			return attributeName;
		}

		return persistenceEntryManager.toInternalAttribute(attributeName);
	}

	private Expression buildTypedExpression(Filter filter) {
/*
		if (filter.getAssertionValue() instanceof Date) {
   	    	return Expressions.constant(new java.sql.Timestamp(((Date) filter.getAssertionValue()).getTime()));
		}
*/
		/*
		if (filter.getAssertionValue() instanceof String) {
   	    	return Expressions.constant((String) filter.getAssertionValue());
   	    } else if (filter.getAssertionValue() instanceof Boolean) {
			return Expressions.constant((Boolean) filter.getAssertionValue());
		} else if (filter.getAssertionValue() instanceof Integer) {
			return Expressions.constant((Integer) filter.getAssertionValue());
		} else if (filter.getAssertionValue() instanceof Long) {
			return Expressions.constant((Long) filter.getAssertionValue());
		}
*/
		return Expressions.constant(filter.getAssertionValue());
	}

	private Expression buildTypedPath(Filter filter, boolean skipAlias) {
		String internalAttribute = toInternalAttribute(filter);
		
		return buildTypedPath(filter, internalAttribute, skipAlias);
	}

	private Expression buildTypedPath(Filter filter, String attributeName, boolean skipAlias) {
   	    if (filter.getAssertionValue() instanceof String) {
   	    	if (skipAlias) {
   	    		return Expressions.stringPath(attributeName);
   	    	} else {
   	    		return Expressions.stringPath(stringDocAlias, attributeName);
   	    	}
   	    } else if (filter.getAssertionValue() instanceof Boolean) {
   	    	if (skipAlias) {
   	   	    	return Expressions.booleanPath(attributeName);
   	    	} else {
   	   	    	return Expressions.booleanPath(booleanDocAlias, attributeName);
   	    	}
		} else if (filter.getAssertionValue() instanceof Integer) {
   	    	if (skipAlias) {
   	   	    	return Expressions.stringPath(attributeName);
   	    	} else {
   	   	    	return Expressions.stringPath(integerDocAlias, attributeName);
   	    	}
		} else if (filter.getAssertionValue() instanceof Long) {
   	    	if (skipAlias) {
   	   	    	return Expressions.stringPath(attributeName);
   	    	} else {
   	   	    	return Expressions.stringPath(longDocAlias, attributeName);
   	    	}
		}

    	if (skipAlias) {
    	    return Expressions.stringPath(attributeName);
    	} else {
    	    return Expressions.stringPath(objectDocAlias, attributeName);
    	}
	}

	private Boolean determineMultiValuedByType(String attributeName, Map<String, PropertyAnnotation> propertiesAnnotationsMap) {
		if ((attributeName == null) || (propertiesAnnotationsMap == null)) {
			return null;
		}

		if (StringHelper.equalsIgnoreCase(attributeName, SqlEntryManager.OBJECT_CLASS)) {
			return false;
		}

		PropertyAnnotation propertyAnnotation = propertiesAnnotationsMap.get(attributeName);
		if ((propertyAnnotation == null) || (propertyAnnotation.getParameterType() == null)) {
			return null;
		}

		Class<?> parameterType = propertyAnnotation.getParameterType();
		
		boolean isMultiValued = parameterType.equals(String[].class) || ReflectHelper.assignableFrom(parameterType, List.class) || ReflectHelper.assignableFrom(parameterType, AttributeEnum[].class);
		
		return isMultiValued;
	}

	private boolean isRequiredConsistency(Filter filter, Map<String, PropertyAnnotation> propertiesAnnotationsMap) {
		if (propertiesAnnotationsMap == null) {
			return false;
		}

		String attributeName = filter.getAttributeName();
    	PropertyAnnotation propertyAnnotation = propertiesAnnotationsMap.get(attributeName);
		if ((propertyAnnotation == null) || (propertyAnnotation.getParameterType() == null)) {
			return false;
		}
		AttributeName attributeNameAnnotation = (AttributeName) ReflectHelper.getAnnotationByType(propertyAnnotation.getAnnotations(),
				AttributeName.class);
		
		if (attributeNameAnnotation.consistency()) {
			return true;
		}

		return false;
	}

}
