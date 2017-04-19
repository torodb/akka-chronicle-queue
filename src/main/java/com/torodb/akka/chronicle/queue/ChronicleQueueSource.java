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

import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.stream.stage.TimerGraphStageLogic;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import scala.concurrent.duration.FiniteDuration;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * An Akka offheap source that retrieve messages from a {@link ChronicleQueue}.
 *
 * ''Emits''' the buffer has an element available
 *
 * '''Completes''' never
 */
public class ChronicleQueueSource<T> extends GraphStage<SourceShape<T>> {

  private final Outlet<T> out = Outlet.create(ChronicleQueueSource.class.getSimpleName() + ".out");
  private final SourceShape<T> shape = SourceShape.of(out);
  private final ChronicleQueue queue;
  private final ReadMarshaller<T> reader;
  private final Supplier<FiniteDuration> pollingIntervalSupplier;

  ChronicleQueueSource(ChronicleQueue queue, ReadMarshaller<T> reader,
      Supplier<FiniteDuration> pollingIntervalSupplier) {
    this.queue = queue;
    this.reader = reader;
    this.pollingIntervalSupplier = pollingIntervalSupplier;
  }

  ChronicleQueue getQueue() {
    return queue;
  }

  @Override
  public SourceShape<T> shape() {
    return shape;
  }

  @Override
  public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
    return new Logic();
  }

  class Logic extends TimerGraphStageLogic {

    private final ExcerptTailer tailer;

    public Logic() {
      super(shape());

      this.tailer = queue.createTailer();

      setHandler(out, this::doPull);
    }

    private void doPull() {
      Optional<T> element = readElement();
      if (element.isPresent()) {
        push(out, element.get());
      } else {
        if (queue.isClosed()) {
          completeStage();
        } else {
          scheduleOnce("poll", pollingIntervalSupplier.get());
        }
      }
    }

    @Override
    public void onTimer(Object timerKey) throws Exception {
      doPull();
    }

    private Optional<T> readElement() {
      try (DocumentContext dc = tailer.readingDocument()) {
        if (!dc.isPresent()) {
          return Optional.empty();
        }
        return Optional.of(
            reader.apply(dc.wire())
        );
      }
    }
  }


}
