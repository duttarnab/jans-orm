package io.jans.persist.couchbase.operation.watch;

import io.jans.persist.watch.DurationUtil;

/**
 * Simple Couchbase operation duration calculator helper
 *
 * @author Yuriy Movchan Date: 04/08/2019
 */
public class OperationDurationUtil extends DurationUtil {

    private static OperationDurationUtil instance = new OperationDurationUtil();

    public static DurationUtil instance() {
    	return instance;
    }

    public void logDebug(String format, Object... arguments) {
        if (log.isDebugEnabled()) {
            log.debug(format, arguments);
        }
    }

}
