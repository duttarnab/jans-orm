/*
 * Janssen Project software is available under the MIT License (2008). See http://opensource.org/licenses/MIT for full text.
 *
 * Copyright (c) 2020, Janssen Project
 */

package io.jans.sql.test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.jans.orm.sql.impl.SqlEntryManager;
import io.jans.orm.sql.impl.SqlEntryManagerFactory;
import io.jans.orm.util.Pair;

/**
 *
 * @author Yuriy Movchan Date: 01/15/2020
 */
public class ManualSqlEntryManagerTest {
	
	private SqlEntryManager manager;
	private SessionId persistedSessionId;
	
	@BeforeClass(enabled = false)
	public void init() throws IOException {
        manager = createSqlEntryManager();
	}

	@AfterClass(enabled = false)
	public void destroy() throws IOException {
		if (manager != null) {
			manager.destroy();
		}
	}

    @Test(enabled = false)
    public void createSessionId() throws IOException {
    	SessionId sessionId = buildSessionId();
        manager.persist(sessionId);
        
        persistedSessionId = sessionId;

        System.out.println(sessionId);
    }

    @Test(dependsOnMethods =  "createSessionId", enabled = false)
    public void updateSessionId() throws IOException {
    	SessionId sessionId = persistedSessionId;
    	
    	
    	Pair<Date, Integer> expirarion = expirationDate(new Date());
    	sessionId.setAuthenticationTime(new Date());
        sessionId.setLastUsedAt(new Date());

        sessionId.setJwt(null);
        sessionId.setIsJwt(null);

        sessionId.setExpirationDate(expirarion.getFirst());
        sessionId.setTtl(expirarion.getSecond());

        manager.merge(sessionId);

        System.out.println(sessionId);
    }

    @Test(dependsOnMethods =  "updateSessionId", enabled = false)
    public void searchSessionId() throws IOException {
        List<SessionId> sessionIdList = manager.findEntries("o=jans", SessionId.class, null);
        System.out.println(sessionIdList);
    }

    private SessionId buildSessionId() {
        SessionId sessionId = new SessionId();
        sessionId.setId(UUID.randomUUID().toString());
        sessionId.setDn(String.format("jansId=%s,%s", sessionId.getId(), "ou=sessions,o=jans"));
        sessionId.setCreationDate(new Date());
        sessionId.setJwt("{}");
        sessionId.setIsJwt(true);

        return sessionId;
    }

    private Pair<Date, Integer> expirationDate(Date creationDate) {
        int expirationInSeconds = 120;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(creationDate);
        calendar.add(Calendar.SECOND, expirationInSeconds);
        return new Pair<>(calendar.getTime(), expirationInSeconds);
    }

    // MODIFY ACCORDING TO YOUR SERVER
    public static Properties loadProperties() throws IOException {
        Properties properties = new Properties();

        try (InputStream is = new FileInputStream("V://Development//jans//conf/jans-sql.properties")) {
//        try (InputStream is = ManualSqlEntryManagerTest.class.getResourceAsStream("sql-backend.jans.io.properties")) {
            Properties props = new Properties();
            props.load(is);

            Iterator<?> keys = props.keySet().iterator();
            while (keys.hasNext()) {
                String key = (String) keys.next();
                String value = (String) props.getProperty(key);
                
                if (!key.startsWith("sql")) {
                	key = "sql." + key;
                }
                properties.put(key,  value);
            }
        }

        properties.put("sql.auth.userName", "root");
        properties.put("sql.auth.userPassword", "Secret1!");

        return properties;
    }

    public SqlEntryManager createSqlEntryManager() throws IOException {
    	SqlEntryManagerFactory sqlEntryManagerFactory = new SqlEntryManagerFactory();
    	sqlEntryManagerFactory.create();

        SqlEntryManager sqlEntryManager = sqlEntryManagerFactory.createEntryManager(loadProperties());
        System.out.println("Created SqlEntryManager: " + sqlEntryManager);

        return sqlEntryManager;
    }
}
