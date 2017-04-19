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

import akka.NotUsed;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Source;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;

/**
 *
 */
@RunWith(JUnitPlatform.class)
@ExtendWith(ChronicleQueueExtension.class)
public abstract class ChronicleQueueSinkContract extends AbstractChronicleQueueStreamContract {

  public abstract ChronicleQueueSink<Element> createSink(ChronicleQueue queue);

  @Test
  public void onDefault(ChronicleQueue queue) {
    int maxElements = 100;

    ChronicleQueueSink<Element> sink = createSink(queue);
    CompletableFuture<NotUsed> future = Source.range(1, maxElements)
        .map(Element::new)
        .toMat(sink, Keep.right())
        .run(getMaterializer());

    future.join();

    ExcerptTailer tailer = queue.createTailer();
    ReadMarshaller<Element> readMarshaller = new Element.ElementMarshaller();
    for (int i = 1; i <= maxElements; i++) {
      try (DocumentContext dc = tailer.readingDocument()) {
        Assertions.assertTrue(dc.isPresent(), "The " + i + "th document cannot be found");
        Element element = readMarshaller.apply(dc.wire());
        Assertions.assertEquals(i, element.getValue(), "The " + i + "th document is incorrect");
      }
    }

    try (DocumentContext dc = tailer.readingDocument()) {
      Assertions.assertFalse(dc.isPresent(), "There are more than " + maxElements + " elements on "
          + "the queue");
    }
  }

}
