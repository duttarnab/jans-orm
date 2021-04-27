/*
 * Janssen Project software is available under the MIT License (2008). See http://opensource.org/licenses/MIT for full text.
 *
 * Copyright (c) 2020, Janssen Project
 */

package io.jans.orm.cloud.spanner.persistence;

import java.util.Properties;

import org.apache.log4j.Logger;

import io.jans.orm.cloud.spanner.impl.SpannerEntryManager;
import io.jans.orm.cloud.spanner.impl.SpannerEntryManagerFactory;

/**
 * @author Yuriy Movchan Date: 01/15/2020
 */
public class SpannerEntryManagerSample {

    private static final Logger LOG = Logger.getLogger(SpannerEntryManagerSample.class);

    private Properties getSampleConnectionProperties() {
        Properties connectionProperties = new Properties();

        connectionProperties.put("spanner#db.schema.name", "jans");
        connectionProperties.put("spanner#connection.uri", "jdbc:mysql://localhost:3306/jans?profileSQL=true");

        connectionProperties.put("spanner#connection.driver-property.serverTimezone", "GMT+2");
        connectionProperties.put("spanner#connection.pool.max-total", "300");
        connectionProperties.put("spanner#connection.pool.max-idle", "300");

        connectionProperties.put("spanner#auth.userName", "jans");
        connectionProperties.put("spanner#auth.userPassword", "secret");
        
        // Password hash method
        connectionProperties.put("spanner#password.encryption.method", "SSHA-256");
        
        // Max time needed to create connection pool in milliseconds
        connectionProperties.put("spanner#connection.pool.create-max-wait-time-millis", "20000");
        
        // Max wait 20 seconds
        connectionProperties.put("spanner#connection.pool.max-wait-time-millis", "20000");
        
        // Allow to evict connection in pool after 30 minutes
        connectionProperties.put("spanner#connection.pool.min-evictable-idle-time-millis", "1800000");

        connectionProperties.put("spanner#binaryAttributes", "objectGUID");
        connectionProperties.put("spanner#certificateAttributes", "userCertificate");

        return connectionProperties;
    }

    public SpannerEntryManager createSpannerEntryManager() {
        SpannerEntryManagerFactory sqlEntryManagerFactory = new SpannerEntryManagerFactory();
        sqlEntryManagerFactory.create();
        Properties connectionProperties = getSampleConnectionProperties();

        SpannerEntryManager sqlEntryManager = sqlEntryManagerFactory.createEntryManager(connectionProperties);
        LOG.debug("Created SpannerEntryManager: " + sqlEntryManager);

        return sqlEntryManager;
    }

}