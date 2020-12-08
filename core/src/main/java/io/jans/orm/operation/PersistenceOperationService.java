/*
 * Janssen Project software is available under the Apache License (2004). See http://www.apache.org/licenses/ for full text.
 *
 * Copyright (c) 2020, Janssen Project
 */

package io.jans.orm.operation;

import io.jans.orm.exception.extension.PersistenceExtension;

/**
 * Base interface for Operation Service
 *
 * @author Yuriy Movchan Date: 06/22/2018
 */
public interface PersistenceOperationService {

    boolean isConnected();

	public void setPersistenceExtension(PersistenceExtension persistenceExtension);

}
