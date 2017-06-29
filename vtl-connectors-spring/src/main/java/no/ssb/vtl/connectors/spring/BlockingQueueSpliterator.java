package no.ssb.vtl.connectors.spring;

/*-
 * ========================LICENSE_START=================================
 * Java VTL Spring connector
 * %%
 * Copyright (C) 2017 Statistics Norway and contributors
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import no.ssb.vtl.model.DataPoint;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Created by hadrien on 21/06/2017.
 */
class BlockingQueueSpliterator extends Spliterators.AbstractSpliterator<DataPoint> {

    // End of stream marker.
    public static final DataPoint EOS = DataPoint.create(0);

    private final BlockingQueue<DataPoint> queue;
    private final Future<?> future;
    private final AtomicReference<Exception> exception;
    private boolean hasMore = true;

    public BlockingQueueSpliterator(BlockingQueue<DataPoint> queue, Future<?> future, AtomicReference<Exception> exception) {
        super(Long.MAX_VALUE, Spliterator.IMMUTABLE);
        this.queue = queue;
        this.future = future;
        this.exception = exception;
    }

    @Override
    public boolean tryAdvance(Consumer<? super DataPoint> action) {
        if (!hasMore)
            return false;

        try {

            DataPoint p = queue.take();
            if (p == EOS) {
                hasMore = false;
                return false;
            }

            action.accept(p);

        } catch (InterruptedException ie) {
            future.cancel(true);
            Thread.currentThread().interrupt();

            Exception ex = this.exception.get();
            if (ex != null)
                throw new RuntimeException(ex);
            else
                throw new RuntimeException("stream interrupted");
        }
        return true;
    }

    @Override
    public void forEachRemaining(Consumer<? super DataPoint> action) {
        try {

            hasMore = false;

            DataPoint p;
            while ((p = queue.take()) != EOS)
                action.accept(p);

        } catch (InterruptedException ie) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new RuntimeException("stream interrupted");
        }
    }
}
