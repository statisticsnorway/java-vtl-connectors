package no.ssb.vtl.tools.sandbox.connector.util;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A Spliterator that can be shared between threads.
 */
public class ConcurrentSpliterator<T> implements Supplier<Spliterator<T>> {

    private final Supplier<Spliterator<T>> supplier;


    private final ArrayList<T> data;
    private final int limit;
    private volatile int size = -1;

    private Spliterator<T> spliterator;

    private ReentrantLock writeLock = new ReentrantLock();

    private ConcurrentSpliterator(int limit, Supplier<Spliterator<T>> supplier) {
        this.limit = checkNotNull(limit);
        this.supplier = checkNotNull(supplier);
        this.spliterator = supplier.get();
        this.data = Lists.newArrayListWithCapacity(limit);
    }

    public synchronized static <T> ConcurrentSpliterator<T> create(int limit, Supplier<Spliterator<T>> supplier) {
        return new ConcurrentSpliterator<>(limit, supplier);
    }

    @Override
    public Spliterator<T> get() {
        return new SharedSpliterator();
    }

    private class SkipSpliterator implements Spliterator<T> {

        private final Spliterator<T> source;
        int skip = -1;

        public SkipSpliterator(int skip, Spliterator<T> spliterator) {
            this.skip = skip;
            this.source = spliterator;
        }


        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            if (skip <= 0) {
                return source.tryAdvance(action);
            }
            while (skip > 0 && source.tryAdvance(e -> {
                skip--;
                action.accept(e);
            })) ;
            return skip <= 0;
        }

        @Override
        public void forEachRemaining(Consumer<? super T> action) {
            if (skip > 0) {
                tryAdvance(action);
            }
            if (skip <= 0) {
                source.forEachRemaining(action);
            }
        }

        @Override
        public Spliterator<T> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return skip <= 0 ? source.estimateSize() : Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return source.characteristics() & ~Spliterator.SIZED;
        }

        @Override
        public Comparator<? super T> getComparator() {
            return source.getComparator();
        }
    }

    private class SharedSpliterator extends Spliterators.AbstractSpliterator<T> {

        private int counter = -1;
        private boolean hasMore = true;
        private Spliterator<T> source;


        private SharedSpliterator() {
            super(Long.MAX_VALUE, Spliterator.IMMUTABLE);
            source = spliterator;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {

            if (source != spliterator)
                return source.tryAdvance(action);

            if (!hasMore) // && !(counter < limit))
                return false;

            if (writeLock.tryLock()) {
                try {
                    if (size >= limit)
                        source = new SkipSpliterator(counter, supplier.get());

                    hasMore &= source.tryAdvance(dp -> {
                        data.add(dp);
                        action.accept(dp);
                    });
                    size = data.size();

                    return hasMore;
                } finally {
                    writeLock.unlock();
                }
            } else {
                writeLock.lock();
                try {
                    if (size >= limit)
                        source = new SkipSpliterator(counter, supplier.get());

                    while (++counter < size) {
                        T next = data.get(counter);
                        action.accept(next);
                    }
                    return true;
                } finally {
                    writeLock.unlock();
                }
            }

        }
    }
}
