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
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;

import java.util.Optional;

/**
 *
 */
public class ChronicleQueueBuffer<T> extends GraphStage<FlowShape<T, Event<T>>> {

  private final ChronicleQueue queue;
  private final WriteMarshaller<T> writer;
  private final ReadMarshaller<T> reader;
  private final Inlet<T> in = Inlet.create(ChronicleQueueBuffer.class.getSimpleName() + ".in");
  private final Outlet<Event<T>> out = Outlet.create(
      ChronicleQueueBuffer.class.getSimpleName() + ".out");
  private final FlowShape<T, Event<T>> shape = FlowShape.of(in, out);

  ChronicleQueueBuffer(ChronicleQueue queue, WriteMarshaller<T> writer, ReadMarshaller<T> reader) {
    this.queue = queue;
    this.writer = writer;
    this.reader = reader;
  }

  ChronicleQueue getQueue() {
    return queue;
  }

  @Override
  public FlowShape<T, Event<T>> shape() {
    return shape;
  }

  @Override
  public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
    return new Logic();
  }

  protected class Logic extends GraphStageLogic {
    private final ExcerptAppender appender;
    private final ExcerptTailer tailer;

    public Logic() {
      super(shape());

      appender = queue.acquireAppender();
      tailer = queue.createTailer();

      setHandler(in, this::onPush);
      setHandler(out, this::onPull);
    }

    @Override
    public void preStart() throws Exception {
      pull(in);
    }

    private void onPush() {
      T elem = grab(in);

      appender.writeDocument(wire -> writer.accept(wire, elem));
      if (isAvailable(out)) {
        onPull();
      }
      pull(in);
    }

    private void onPull() {
      Optional<Event<T>> element = readElement();
      if (!element.isPresent()) {
        //There are no elements on the queue, we have to wait until a push
        return ;
      }
      push(out, element.get());
    }

    private Optional<Event<T>> readElement() {
      try (DocumentContext dc = tailer.readingDocument()) {
        if (!dc.isPresent()) {
          return Optional.empty();
        }
        return Optional.of(
            new Event<>(
                dc.index(),
                reader.apply(dc.wire())
            )
        );
      }
    }
  }
}
