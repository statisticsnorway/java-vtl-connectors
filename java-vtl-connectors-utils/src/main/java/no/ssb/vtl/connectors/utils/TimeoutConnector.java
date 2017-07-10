package no.ssb.vtl.connectors.utils;

/*-
 * ========================LICENSE_START=================================
 * Java VTL Utility connectors
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

import no.ssb.vtl.connectors.Connector;
import no.ssb.vtl.connectors.ConnectorException;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link Connector} wrapper that makes sure all streams returned by the datasets
 * will eventually timeout.
 */
public class TimeoutConnector extends ForwardingConnector {

    public static final String DELEGATE_WAS_NULL_ERROR = "delegate was null";
    public static final String UNIT_WAS_NULL_ERROR = "unit was null";
    private static final String TIMEOUT_ZERO_ERROR = "timeout must be greater than 0";
    private static final Logger logger = LoggerFactory.getLogger(TimeoutConnector.class);

    private final long timeout;
    private final TimeUnit unit;
    private final ScheduledExecutorService scheduler;

    private final Connector delegate;

    private TimeoutConnector(Connector delegate, long timeout, TimeUnit unit) {
        this.delegate = checkNotNull(delegate, DELEGATE_WAS_NULL_ERROR);
        this.timeout = timeout;
        checkArgument(timeout > 0, TIMEOUT_ZERO_ERROR);
        this.unit = checkNotNull(unit, UNIT_WAS_NULL_ERROR);
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * Create a cancel task
     */
    private static Runnable createCancelTask(ScheduledFuture<?> timeoutTask, Stream<DataPoint> stream, Dataset dataset) {
        return () -> {
            if (!timeoutTask.isCancelled()) {
                logger.debug("stream {} closed, cancelling the scheduled task {}", stream, timeoutTask);
                timeoutTask.cancel(true);
            } else {
                logger.debug("the task {} was already cancelled");
            }
        };
    }

    @Override
    protected Connector delegate() {
        return this.delegate;
    }

    @Override
    public Dataset getDataset(String identifier) throws ConnectorException {
        Dataset dataset = super.getDataset(identifier);
        return new ForwardingDataset() {

            @Override
            protected Dataset delegate() {
                return dataset;
            }

            @Override
            public Stream<DataPoint> getData() {
                Stream<DataPoint> dataStream = super.getData();
                // Register timeout task and cancel it if the stream is closed.
                ScheduledFuture<?> timeoutTask = scheduleTimeoutFor(dataStream, this);

                return dataStream.onClose(
                        createCancelTask(
                                timeoutTask, dataStream, this
                        )
                );
            }
        };
    }

    /**
     * Schedule a task that will close the given stream.
     */
    private ScheduledFuture<?> scheduleTimeoutFor(Stream<DataPoint> dataStream, Dataset dataset) {

        if (logger.isDebugEnabled()) {
            Duration duration = Duration.of(this.unit.toNanos(this.timeout), ChronoUnit.NANOS);
            Instant time = Instant.now().plus(duration);
            logger.debug("scheduling close time at {} ({} from now)", time, duration);
        }

        return scheduler.schedule(createCloseTask(dataStream, dataset), this.timeout, this.unit);
    }

    /**
     * Create a Runnable that will close and log a message
     */
    private Runnable createCloseTask(Stream<DataPoint> dataStream, Dataset dataset) {
        return () -> {
            logger.warn("closing stream {} of dataset {}", dataStream, dataset);
            dataStream.close();
        };
    }

    public static TimeoutConnector create(Connector connector, long timeout, TimeUnit unit) {
        return new TimeoutConnector(connector, timeout, unit);
    }

    @Override
    protected void finalize() throws Throwable {
        this.scheduler.shutdown();
    }
}
