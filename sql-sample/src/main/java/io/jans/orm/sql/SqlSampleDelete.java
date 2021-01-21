/*
 * Janssen Project software is available under the MIT License (2008). See http://opensource.org/licenses/MIT for full text.
 *
 * Copyright (c) 2020, Janssen Project
 */

package io.jans.orm.sql;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jans.orm.model.base.DeletableEntity;
import io.jans.orm.search.filter.Filter;
import io.jans.orm.sql.impl.SqlEntryManager;
import io.jans.orm.sql.operation.impl.SqlConnectionProvider;
import io.jans.orm.sql.persistence.SqlSampleEntryManager;

/**
 * @author Yuriy Movchan Date: 01/15/2020
 */
public final class SqlSampleDelete {

    private static final Logger LOG = LoggerFactory.getLogger(SqlConnectionProvider.class);

    private SqlSampleDelete() {
    }

    public static void main(String[] args) {
        // Prepare sample connection details
        SqlSampleEntryManager sqlSampleEntryManager = new SqlSampleEntryManager();

        // Create SQL entry manager
        SqlEntryManager sqlEntryManager = sqlSampleEntryManager.createSqlEntryManager();

        String baseDn = "ou=cache,o=jans";
		Filter filter = Filter.createANDFilter(
		        Filter.createEqualityFilter("del", true),
				Filter.createLessOrEqualFilter("exp", sqlEntryManager.encodeTime(baseDn, new Date()))
        );

        int result = sqlEntryManager.remove(baseDn, DeletableEntity.class, filter, 100);
        System.out.println(result);
    }

}
