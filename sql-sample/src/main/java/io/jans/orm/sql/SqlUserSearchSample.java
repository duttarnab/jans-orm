/*
 * Janssen Project software is available under the MIT License (2008). See http://opensource.org/licenses/MIT for full text.
 *
 * Copyright (c) 2020, Janssen Project
 */

package io.jans.orm.sql;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.status.StatusLogger;

import io.jans.orm.search.filter.Filter;
import io.jans.orm.sql.impl.SqlEntryManager;
import io.jans.orm.sql.model.SimpleUser;
import io.jans.orm.sql.persistence.SqlSampleEntryManager;
import io.jans.orm.util.StringHelper;

/**
 * @author Yuriy Movchan Date: 01/15/2020
 */
public final class SqlUserSearchSample {

    private static final Logger LOG;

    static {
        StatusLogger.getLogger().setLevel(Level.OFF);
        LoggingHelper.configureConsoleAppender();
        LOG = Logger.getLogger(SqlUserSearchSample.class);
    }

    private static AtomicLong successResult = new AtomicLong(0) ;
    private static AtomicLong failedResult = new AtomicLong(0) ;
    private static AtomicLong errorResult = new AtomicLong(0) ;
    private static AtomicLong totalTime = new AtomicLong(0) ;
    private static AtomicLong activeCount = new AtomicLong(0) ;

    private SqlUserSearchSample() {
    }

    public static void main(String[] args) throws InterruptedException {
        // Prepare sample connection details
        SqlSampleEntryManager sqlSampleEntryManager = new SqlSampleEntryManager();
        final SqlEntryManager sqlEntryManager = sqlSampleEntryManager.createSqlEntryManager();
        
        int countUsers = 1000000;
        int threadCount = 200;
        int threadIterationCount = 10;

    	long totalStart = System.currentTimeMillis();
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(threadCount, daemonThreadFactory());
            for (int i = 0; i < threadCount; i++) {
            	activeCount.incrementAndGet();
                final int count = i;
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                    	long start = System.currentTimeMillis();
                        for (int j = 0; j < threadIterationCount; j++) {
	                    	long userUid = Math.round(Math.random() * countUsers);
	                    	String uid = String.format("user%06d", userUid);
	                        try {
		                        Filter filter = Filter.createEqualityFilter(Filter.createLowercaseFilter("uid"), StringHelper.toLowerCase(uid));
//		                        Filter filter = Filter.createEqualityFilter("uid", uid);
		                        List<SimpleUser> foundUsers = sqlEntryManager.findEntries("ou=people,o=jans", SimpleUser.class, filter);
		                        if (foundUsers.size() > 0) {
		                        	successResult.incrementAndGet();
		                        } else {
		                        	LOG.warn("Failed to find user: " + uid);
		                        	failedResult.incrementAndGet();
		                        }
	                        } catch (Throwable e) {
	                        	errorResult.incrementAndGet();
	                            System.out.println("ERROR !!!, thread: " + count + ", uid: " + uid + ", error:" + e.getMessage());
	                            e.printStackTrace();
	                        }
                        }
                        
                        long end = System.currentTimeMillis();
                        long duration = end - start; 
                        LOG.info("Thread " + count + " execution time: " + duration);
                        totalTime.addAndGet(duration);
                    	activeCount.decrementAndGet();
                    }
                });
            }

            while (activeCount.get() != 0) {
            	Thread.sleep(1000L);
            }
        } finally {
            sqlEntryManager.destroy();
        }
        long totalEnd = System.currentTimeMillis();
        long duration = totalEnd - totalStart; 

        LOG.info("Total execution time: " + duration + " after execution: " + (threadCount * threadIterationCount));

        System.out.println(String.format("successResult: '%d', failedResult: '%d', errorResult: '%d'", successResult.get(), failedResult.get(), errorResult.get()));
    }

    public static ThreadFactory daemonThreadFactory() {
        return new ThreadFactory() {
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable);
                thread.setDaemon(true);
                return thread;
            }
        };
    }

}
