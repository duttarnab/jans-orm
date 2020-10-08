/*
 * oxCore is available under the MIT License (2008). See http://opensource.org/licenses/MIT for full text.
 *
 * Copyright (c) 2018, Gluu
 */

package io.jans.persist;

import java.util.Map;
import java.util.Properties;

import io.jans.persist.service.BaseFactoryService;

/**
 * Factory which creates Persistence Entry Manager
 *
 * @author Yuriy Movchan Date: 02/02/2018
 */
public interface PersistenceEntryManagerFactory {

	void initStandalone(BaseFactoryService persistanceFactoryService);

    String getPersistenceType();

    Map<String, String> getConfigurationFileNames();

    PersistenceEntryManager createEntryManager(Properties conf);

}
