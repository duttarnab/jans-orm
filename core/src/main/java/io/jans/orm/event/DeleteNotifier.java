/*
 * oxCore is available under the MIT License (2008). See http://opensource.org/licenses/MIT for full text.
 *
 * Copyright (c) 2014, Gluu
 */

package io.jans.orm.event;

public interface DeleteNotifier {

    void onBeforeRemove(String dn);

    void onAfterRemove(String dn);

}