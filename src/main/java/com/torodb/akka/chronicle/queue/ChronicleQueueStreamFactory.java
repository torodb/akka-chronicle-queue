/*
 * Copyright 2017 8Kdata Technology
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.torodb.akka.chronicle.queue;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import scala.concurrent.duration.FiniteDuration;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A factory/DSL used to create sources, buffers and sinks that delegates on a
 * {@link ChronicleQueue}.
 *
 * <pre>
 * Marshaller&lt;YourObject&gt; marshaller = ...
 * ChronicleQueueBuffer&lt;YourObject&gt; buffer = new ChronicleQueueStreamFactory()
 *   .withTemporalQueue()
 *   .manuallyManaged()
 *   .createBuffer(marshaller);
 *
 * </pre>
 */
public class ChronicleQueueStreamFactory<T> {

  /**
   * Starts the creation with a temporally {@link ChronicleQueue}.
   * 
   * @see TemporalChronicleQueueFactory#createTemporalQueue()
   */
  public ManagedBy withTemporalQueue() {
    return withQueue(TemporalChronicleQueueFactory.createTemporalQueue());
  }

  /**
   * Starts the creation with a given queue.
   *
   * @param queue The queue that will be used. Please remember that it is not
   *              <a href="https://github.com/OpenHFT/Chronicle-Queue/blob/master/docs/FAQ.adoc#can-i-have-multiple-readers">recommended
   *              to never open more than one tailer or appender for single {ChronicleQueue
   *              object.</a>
   *
   */
  public ManagedBy withQueue(ChronicleQueue queue) {
    return withQueue(() -> queue);
  }

  /**
   * Starts the creation with a queue that will be obtained by calling a supplier.
   *
   * @param queueSupplier The supplier that will supply the queue that will be used. Please remember
   *                      that it is not
   * <a href="https://github.com/OpenHFT/Chronicle-Queue/blob/master/docs/FAQ.adoc#can-i-have-multiple-readers">recommended
   * to never open more than one tailer or appender for single {ChronicleQueue object.</a>
   */
  public ManagedBy withQueue(Supplier<ChronicleQueue> queueSupplier) {
    return new ManagedBy(queueSupplier);
  }

  /**
   * Starts the creation with a queue that will be obtained by calling a
   * {@link ChronicleQueueBuilder}.
   *
   * @param queueBuilder The builder that will be used to create the queue. Please remember that it
   *                     is not
   * <a href="https://github.com/OpenHFT/Chronicle-Queue/blob/master/docs/FAQ.adoc#can-i-have-multiple-readers">recommended
   * to never open more than one tailer or appender for single {ChronicleQueue object.</a>
   */
  public ManagedBy withQueue(ChronicleQueueBuilder<?> queueBuilder) {
    return withQueue(queueBuilder::build);
  }

  public static class ManagedBy {
    private final Supplier<ChronicleQueue> queueSupplier;

    ManagedBy(Supplier<ChronicleQueue> queueSupplier) {
      this.queueSupplier = queueSupplier;
    }

    /**
     * Indicates that the returned shape will automatically close the queue once the shape
     * completes.
     */
    public ChooseShape autoManaged() {
      return new ChooseShape(queueSupplier, true);
    }

    /**
     * Indicates that the returned shape will not close the queue once the shape completes.
     */
    public ChooseShape manuallyManaged() {
      return new ChooseShape(queueSupplier, false);
    }
  }

  public static class ChooseShape {
    private final Supplier<ChronicleQueue> queueSupplier;
    private final boolean autoManaged;

    ChooseShape(Supplier<ChronicleQueue> queueSupplier, boolean autoManaged) {
      this.queueSupplier = queueSupplier;
      this.autoManaged = autoManaged;
    }

    /**
     * Creates a buffer.
     *
     * @param marshaller the marshaller that will be used to write and read elements into/from the
     *                   queue
     * @return
     */
    public <T> ChronicleQueueBuffer<T> createBuffer(Marshaller<T> marshaller) {
      return createBuffer(marshaller, marshaller);
    }

    /**
     * Creates a buffer.
     *
     * @param writer the function that will be used to write elements into the queue
     * @param reader the function that will be used to read elements from the queue
     * @return
     */
    public <T> ChronicleQueueBuffer<T> createBuffer(
        WriteMarshaller<T> writer, ReadMarshaller<T> reader) {
      if (autoManaged) {
        return new AutoManagedChronicleQueueBuffer<>(queueSupplier, writer, reader);
      } else {
        return new ChronicleQueueBuffer<>(queueSupplier.get(), writer, reader);
      }
    }

    /**
     * Creates a sink.
     *
     * @param writer the function that will be used to write elements into the queue
     */
    public <T> ChronicleQueueSink<T> createSink(WriteMarshaller<T> writer) {
      if (autoManaged) {
        return new AutoManagedChronicleQueueSink<>(queueSupplier, writer);
      } else {
        return new ChronicleQueueSink<>(queueSupplier.get(), writer);
      }
    }

    private FiniteDuration toFiniteDuration(Duration javaDuration) {
      return new FiniteDuration(javaDuration.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a source.
     *
     * @param reader                  the function that will be used to read elements from the queue
     * @param pollingIntervalSupplier a supplier that will be called each time the downstream
     *                                request a new element and the buffer is empty. The returned
     *                                duration indicates how much time it has to wait to check the
     *                                buffer again
     * @return
     */
    public <T> ChronicleQueueSource<T> createSource(
        ReadMarshaller<T> reader, Supplier<Duration> pollingIntervalSupplier) {
      Supplier<FiniteDuration> actualSupplier = () -> toFiniteDuration(
          pollingIntervalSupplier.get());
      if (autoManaged) {
        return new AutoManagedChronicleQueueSource<>(
            queueSupplier, reader, actualSupplier);
      } else {
        return new ChronicleQueueSource<>(queueSupplier.get(), reader, actualSupplier);
      }
    }

    /**
     * Creates a source.
     *
     * @param reader          the function that will be used to read elements from the queue
     * @param pollingInterval How frequently the stage will poll the buffer looking for new elements
     *                        when it is empty and the downstream request has requested an element.
     */
    public <T> ChronicleQueueSource<T> createSource(
        ReadMarshaller<T> reader, FiniteDuration pollingInterval) {
      return createSourceScala(reader, () -> pollingInterval);
    }

    /**
     * Creates a source.
     *
     * @param reader          the function that will be used to read elements from the queue
     * @param pollingInterval How frequently the stage will poll the buffer looking for new elements
     *                        when it is empty and the downstream request has requested an element.
     */
    public <T> ChronicleQueueSource<T> createSource(
         ReadMarshaller<T> reader, Duration pollingInterval) {
      return createSource(reader, () -> pollingInterval);
    }

    /**
     * Creates a source.
     *
     * @param reader                  the function that will be used to read elements from the queue
     * @param pollingIntervalSupplier a supplier that will be called each time the downstream
     *                                request a new element and the buffer is empty. The returned
     *                                duration indicates how much time it has to wait to check the
     *                                buffer again
     * @return
     */
    public <T> ChronicleQueueSource<T> createSourceScala(
        ReadMarshaller<T> reader, Supplier<FiniteDuration> pollingIntervalSupplier) {
      if (autoManaged) {
        return new AutoManagedChronicleQueueSource<>(
            queueSupplier, reader, pollingIntervalSupplier);
      } else {
        return new ChronicleQueueSource<>(queueSupplier.get(), reader, pollingIntervalSupplier);
      }
    }
  }
}
