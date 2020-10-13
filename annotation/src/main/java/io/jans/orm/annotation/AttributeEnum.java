/*
 * Janssen Project software is available under the MIT License (2008). See http://opensource.org/licenses/MIT for full text.
 *
 * Copyright (c) 2020, Janssen Project
 */

package io.jans.orm.annotation;

/**
 * Base interface for Persistance enumerations
 *
 * @author Yuriy Movchan Date: 10.07.2010
 */
public interface AttributeEnum {

    String getValue();

    Enum<? extends AttributeEnum> resolveByValue(String value);

}
