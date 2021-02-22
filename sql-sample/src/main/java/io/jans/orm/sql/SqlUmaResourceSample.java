/*
 * Janssen Project software is available under the MIT License (2008). See http://opensource.org/licenses/MIT for full text.
 *
 * Copyright (c) 2020, Janssen Project
 */

package io.jans.orm.sql;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jans.orm.model.PagedResult;
import io.jans.orm.model.SearchScope;
import io.jans.orm.model.SortOrder;
import io.jans.orm.model.base.CustomObjectAttribute;
import io.jans.orm.search.filter.Filter;
import io.jans.orm.sql.impl.SqlEntryManager;
import io.jans.orm.sql.model.SimpleAttribute;
import io.jans.orm.sql.model.SimpleGrant;
import io.jans.orm.sql.model.SimpleSession;
import io.jans.orm.sql.model.SimpleUser;
import io.jans.orm.sql.model.UmaResource;
import io.jans.orm.sql.operation.impl.SqlConnectionProvider;
import io.jans.orm.sql.persistence.SqlSampleEntryManager;

/**
 * @author Yuriy Movchan Date: 01/15/2020
 */
public final class SqlUmaResourceSample {

    private static final Logger LOG = LoggerFactory.getLogger(SqlConnectionProvider.class);

    private SqlUmaResourceSample() {
    }

    public static void main(String[] args) {
        // Prepare sample connection details
        SqlSampleEntryManager sqlSampleEntryManager = new SqlSampleEntryManager();

        // Create SQL entry manager
        SqlEntryManager sqlEntryManager = sqlSampleEntryManager.createSqlEntryManager();

        final Filter filter = Filter.createEqualityFilter("jansAssociatedClnt", "inum=AB77-1A2B,ou=clients,o=jans").multiValued();
        List<UmaResource> umaResource = sqlEntryManager.findEntries("ou=resources,ou=uma,o=jans", UmaResource.class, filter);

        LOG.info("Found umaResources: " + umaResource);
    }

}
