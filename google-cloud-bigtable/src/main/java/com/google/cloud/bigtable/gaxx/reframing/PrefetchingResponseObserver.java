package com.google.cloud.bigtable.gaxx.reframing;

import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.StateCheckingResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.common.base.Preconditions;
import com.google.common.math.IntMath;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PrefetchingResponseObserver<RespT> extends StateCheckingResponseObserver<RespT> {
    private final AtomicInteger lock = new AtomicInteger();
    private final ResponseObserver<RespT> outerResponseObserver;

    private StreamController innerController;
    private boolean hasStarted;
    private boolean autoFlowControl = true;

    // Read and written by different threads w/o lock.
    private final AtomicInteger numRequested = new AtomicInteger();
    // Written by an application thread and read under lock in delivery.
    private final AtomicReference<Throwable> cancellation = new AtomicReference<>();

    private final int prefetchSize;
    private ConcurrentLinkedQueue<RespT> buffer = new ConcurrentLinkedQueue<>();

    private Throwable error;
    private volatile boolean done;

    // Always written and read by the same thread under lock.
    // Safety flag set in the delivery loop before notifying the outer observer of termination.
    // It's checked by error handling in delivery() to avoid double notifying the outer observer.
    private boolean finished;

    public PrefetchingResponseObserver(ResponseObserver<RespT> outerResponseObserver, int prefetchSize) {
        this.outerResponseObserver = outerResponseObserver;
        this.prefetchSize = prefetchSize;
    }

    @Override
    protected void onStartImpl(StreamController controller) {
        this.innerController = controller;

        // Always disable inner flow control so that we can prefetch
        innerController.disableAutoInboundFlowControl();

        outerResponseObserver.onStart(new StreamController() {
            @Override
            public void disableAutoInboundFlowControl() {
                Preconditions.checkState(
                        !hasStarted, "Can't disable automatic flow control once the stream has started");

                autoFlowControl = false;
                numRequested.set(0);
            }

            @Override
            public void request(int count) {
                PrefetchingResponseObserver.this.onRequest(count);
            }

            @Override
            public void cancel() {
                PrefetchingResponseObserver.this.onCancel();
            }
        });
        hasStarted = true;

        // Immediately request the prefetch count
        innerController.request(prefetchSize);

        // When autoflow control is enabled, start immediate request loop
        if (autoFlowControl) {
            numRequested.set(1);
            innerController.request(1);
            deliver();
        }
    }

    private void onRequest(int count) {
        Preconditions.checkState(!autoFlowControl, "Auto flow control enabled");
        Preconditions.checkArgument(count > 0, "Count must be > 0");

        addRequested(count);
        innerController.request(count);

        deliver();
    }

    private void addRequested(int count) {
        while (true) {
            int current = numRequested.get();
            if (current == Integer.MAX_VALUE) {
                return;
            }

            int newValue = IntMath.saturatedAdd(current, count);
            if (numRequested.compareAndSet(current, newValue)) {
                break;
            }
        }
    }

    /**
     * Cancels the stream and notifies the downstream {@link ResponseObserver#onError(Throwable)}.
     * This method can be called multiple times, but only the first time has any effect. Please note
     * that there is a race condition between cancellation and the stream completing normally.
     */
    private void onCancel() {
        if (cancellation.compareAndSet(null, new CancellationException("User cancelled stream"))) {
            innerController.cancel();
        }

        deliver();
    }

    @Override
    protected void onResponseImpl(RespT response) {
        buffer.add(response);
        if (autoFlowControl) {
            innerController.request(1);
            addRequested(1);
        }
        deliver();
    }

    @Override
    protected void onErrorImpl(Throwable t) {
        error = t;
        done = true;
        deliver();
    }

    @Override
    protected void onCompleteImpl() {
        done = true;
        deliver();
    }

    private void deliver() {
        try {
            deliverUnsafe();
        } catch (Throwable t) {
            // This should never happen. If does, it means we are in an inconsistent state and should
            // close the stream and further processing should be prevented. This is accomplished by
            // purposefully leaving the lock non-zero and notifying the outerResponseObserver of the
            // error. Care must be taken to avoid calling close twice in case the first invocation threw
            // an error.
            try {
                innerController.cancel();
            } catch (Throwable cancelError) {
                t.addSuppressed(
                        new IllegalStateException(
                                "Failed to cancel upstream while recovering from an unexpected error",
                                cancelError));
            }
            if (!finished) {
                outerResponseObserver.onError(t);
            }
        }
    }

    private void deliverUnsafe() {
        // Try to acquire the lock
        if (lock.getAndIncrement() != 0) {
            return;
        }

        do {
            lock.lazySet(1);

            if (maybeFinish()) {
                return;
            }

            // Deliver as many messages as possible
            int demandSnapshot = numRequested.get();
            int delivered = 0;

            while (delivered < demandSnapshot) {
                if (maybeFinish()) {
                    return;
                } else if (!buffer.isEmpty()) {
                    delivered++;
                    outerResponseObserver.onResponse(buffer.remove());
                } else {
                    break;
                }
            }
            if (delivered != 0) {
                numRequested.addAndGet(-delivered);
            }

        } while (lock.decrementAndGet() != 0);
    }

    private boolean maybeFinish() {
        // Eagerly process cancellation
        Throwable localError = this.cancellation.get();
        if (localError != null) {
            finished = true;

            outerResponseObserver.onError(localError);
            return true;
        }

        // Check for upstream termination and exhaustion of local buffers
        if (done && buffer.isEmpty()) {
            finished = true;

            if (error != null) {
                outerResponseObserver.onError(error);
            } else {
                outerResponseObserver.onComplete();
            }
            return true;
        }

        // No termination conditions found, go back to business as usual
        return false;
    }
}
