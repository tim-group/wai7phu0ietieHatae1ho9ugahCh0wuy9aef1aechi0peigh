package com.timgroup.concurrent;

import java.util.concurrent.FutureTask;

public final class SettableFuture<T> extends FutureTask<T> {
    private static final Runnable DO_NOTHING = new Runnable() {
        @Override
        public void run() {}
    };
    
    public SettableFuture() {
        super(DO_NOTHING, null);
    }
    
    @Override
    public void set(T v) {
        super.set(v);
    }
}
