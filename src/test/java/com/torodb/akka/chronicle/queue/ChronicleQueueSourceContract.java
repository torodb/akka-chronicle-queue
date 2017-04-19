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

import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;


/**
 *
 */
@RunWith(JUnitPlatform.class)
@ExtendWith(ChronicleQueueExtension.class)
public abstract class ChronicleQueueSourceContract extends AbstractChronicleQueueStreamContract {

  public abstract ChronicleQueueSource<Element> createSource(ChronicleQueue queue);

  private CompletableFuture<Integer> lookFor(ChronicleQueue queue, int poison, int orElse) {
    return Source.fromGraph(createSource(queue))
        .map(Element::getValue)
        .takeWhile(i -> i != poison)
        .runWith(Sink.lastOption(), getMaterializer())
        .toCompletableFuture()
        .thenApply(optional-> optional.orElse(orElse));
  }

  @Test
  public void closedQueue(ChronicleQueue queue) {
    queue.close();

    CompletableFuture<Integer> maxFuture = lookFor(queue, 0, Integer.MIN_VALUE);
    Assertions.assertEquals((Object) Integer.MIN_VALUE, maxFuture.join());
  }

  @Test
  public void emptyQueue(ChronicleQueue queue) throws InterruptedException {

    int poison = 100;
    int defaultValue = 1;

    CompletableFuture<Integer> maxFuture = lookFor(queue, poison, defaultValue);

    Thread.sleep(100);

    ExcerptAppender appender = queue.acquireAppender();
    new Element.ElementMarshaller().write(appender, new Element(poison));

    Integer result = Assertions.assertTimeoutPreemptively(Duration.ofSeconds(1), maxFuture::join);
    Assertions.assertEquals(Integer.valueOf(defaultValue), result);
  }

  @Test
  public void notEmptyQueue(ChronicleQueue queue) {

    int poison = 100;
    int defaultValue = 1;

    ExcerptAppender appender = queue.acquireAppender();
    new Element.ElementMarshaller().write(appender, new Element(poison));

    CompletableFuture<Integer> maxFuture = lookFor(queue, poison, defaultValue);

    Integer result = Assertions.assertTimeoutPreemptively(Duration.ofSeconds(1), maxFuture::join);
    Assertions.assertEquals(Integer.valueOf(defaultValue), result);
  }

  @Test
  public void severalElementsQueue(ChronicleQueue queue) throws InterruptedException {
    ExcerptAppender appender = queue.acquireAppender();

    Element.ElementMarshaller marshaller = new Element.ElementMarshaller();

    int poison = 100;
    int defaultValue = 1;

    IntStream.rangeClosed(0, poison)
        .mapToObj(Element::new)
        .forEach(element -> marshaller.write(appender, element));

    CompletableFuture<Integer> maxFuture = lookFor(queue, poison, defaultValue);

    Thread.sleep(1000);

    Integer result = Assertions.assertTimeoutPreemptively(Duration.ofSeconds(1), maxFuture::join);
    Assertions.assertEquals(Integer.valueOf(poison - 1), result);

  }

}
