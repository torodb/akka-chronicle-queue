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
 *
 */
public class ChronicleQueueStreamFactory<T> {

  public ManagedBy withTemporalQueue() {
    return withQueue(TemporalChronicleQueueFactory.createTemporalQueue());
  }

  public ManagedBy withQueue(ChronicleQueue queue) {
    return withQueue(() -> queue);
  }

  public ManagedBy withQueue(Supplier<ChronicleQueue> queueSupplier) {
    return new ManagedBy(queueSupplier);
  }

  public ManagedBy withQueue(ChronicleQueueBuilder<?> queueBuilder) {
    return withQueue(queueBuilder::build);
  }

  public static class ManagedBy {
    private final Supplier<ChronicleQueue> queueSupplier;

    ManagedBy(Supplier<ChronicleQueue> queueSupplier) {
      this.queueSupplier = queueSupplier;
    }

    public ChooseShape autoManaged() {
      return new ChooseShape(queueSupplier, true);
    }

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

    public <T> ChronicleQueueBuffer<T> createBuffer(Marshaller<T> marshaller) {
      return createBuffer(marshaller, marshaller);
    }

    public <T> ChronicleQueueBuffer<T> createBuffer(
        WriteMarshaller<T> writer, ReadMarshaller<T> reader) {
      if (autoManaged) {
        return new AutoManagedChronicleQueueBuffer<>(queueSupplier, writer, reader);
      } else {
        return new ChronicleQueueBuffer<>(queueSupplier.get(), writer, reader);
      }
    }

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

    public <T> ChronicleQueueSource<T> createSource(
         ReadMarshaller<T> reader, FiniteDuration pollingInterval) {
      return createSourceScala(reader, () -> pollingInterval);
    }

    public <T> ChronicleQueueSource<T> createSource(
         ReadMarshaller<T> reader, Duration pollingInterval) {
      return createSource(reader, () -> pollingInterval);
    }

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
