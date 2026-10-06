package com.mfs.serviceplatform.canary;

import java.util.concurrent.atomic.AtomicInteger;

/** The one regression active right now, selected by the admin endpoint. */
public final class RegressionState {
    private final AtomicInteger activeId = new AtomicInteger(0);

    public void set(int id) {
        RegressionCatalog.byId(id); // validates
        activeId.set(id);
    }

    public RegressionCatalog.Spec active() {
        return RegressionCatalog.byId(activeId.get());
    }
}
