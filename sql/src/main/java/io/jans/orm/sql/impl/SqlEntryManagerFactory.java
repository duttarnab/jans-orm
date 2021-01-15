/*
 * Janssen Project software is available under the MIT License (2008). See http://opensource.org/licenses/MIT for full text.
 *
 * Copyright (c) 2020, Janssen Project
 */

package io.jans.orm.sql.impl;

import java.util.HashMap;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jans.orm.PersistenceEntryManagerFactory;
import io.jans.orm.exception.operation.ConfigurationException;
import io.jans.orm.service.BaseFactoryService;
import io.jans.orm.sql.operation.impl.SqlConnectionProvider;
import io.jans.orm.sql.operation.impl.SqlOperationServiceImpl;
import io.jans.orm.util.PropertiesHelper;

/**
 * Couchbase Entry Manager Factory
 *
 * @author Yuriy Movchan Date: 12/18/2020
 */
@ApplicationScoped
public class SqlEntryManagerFactory implements PersistenceEntryManagerFactory {

    public static final String PERSISTENCE_TYPE = "sql";
    public static final String SQL_DEFAULT_PROPERTIES_FILE = "jans-sql.properties";

	private static final Logger LOG = LoggerFactory.getLogger(SqlEntryManagerFactory.class);

    @PostConstruct
    public void create() {
    }

    @PreDestroy
    public void destory() {
    }

    @Override
    public String getPersistenceType() {
        return PERSISTENCE_TYPE;
    }

    @Override
    public HashMap<String, String> getConfigurationFileNames() {
    	HashMap<String, String> confs = new HashMap<String, String>();
    	confs.put(PERSISTENCE_TYPE, SQL_DEFAULT_PROPERTIES_FILE);

    	return confs;
    }

	@Override
    public SqlEntryManager createEntryManager(Properties conf) {
		Properties entryManagerConf = PropertiesHelper.filterProperties(conf, PERSISTENCE_TYPE);

		SqlConnectionProvider connectionProvider = new SqlConnectionProvider(entryManagerConf);
        connectionProvider.create();
        if (!connectionProvider.isCreated()) {
            throw new ConfigurationException(
                    String.format("Failed to create SQL connection pool! Result code: '%s'", connectionProvider.getCreationResultCode()));
        }
        LOG.debug("Created connectionProvider '{}' with code '{}'", connectionProvider, connectionProvider.getCreationResultCode());

        
        SqlEntryManager sqlEntryManager = new SqlEntryManager(new SqlOperationServiceImpl(entryManagerConf, connectionProvider));
        LOG.info("Created SqlEntryManager: {}", sqlEntryManager.getOperationService());

        return sqlEntryManager;
    }

	@Override
	public void initStandalone(BaseFactoryService persistanceFactoryService) {}

}
