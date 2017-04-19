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

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamConverters;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.stream.testkit.javadsl.TestSource;
import net.openhft.chronicle.queue.ChronicleQueue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 */
@RunWith(JUnitPlatform.class)
@ExtendWith(ChronicleQueueExtension.class)
public abstract class ChronicleQueueBufferContract extends AbstractChronicleQueueStreamContract {

  public abstract ChronicleQueueBuffer<Element> createBuffer(ChronicleQueue queue);

  @Test
  void onDefault(ChronicleQueue queue) {
    ChronicleQueueBuffer<Element> buffer = createBuffer(queue);

    int maxElements = 100;

    List<Integer> result = Source.range(1, maxElements)
        .map(Element::new)
        .via(buffer)
        .map(Excerpt::getElement)
        .map(Element::getValue)
        .runWith(StreamConverters.javaCollector(Collectors::toList), getMaterializer())
        .toCompletableFuture()
        .join();

    Assertions.assertEquals(
        IntStream.range(1, maxElements + 1).boxed().collect(Collectors.toList()),
        result
    );

  }

  @Test
  void emptySource(ChronicleQueue queue, ActorSystem system) {

    Pair<TestPublisher.Probe<Integer>, TestSubscriber.Probe<Integer>> probes =
        createProbes(queue, system);
    TestPublisher.Probe<Integer> sourceProbe = probes.first();
    TestSubscriber.Probe<Integer> sinkProbe = probes.second();

    sourceProbe.sendComplete();
    sinkProbe.request(10)
        .expectComplete();

  }

  @Test
  void cancelledProbe(ChronicleQueue queue, ActorSystem system) {

    ChronicleQueueBuffer<Element> buffer = createBuffer(queue);

    TestSource.<Integer>probe(system)
        .map(Element::new)
        .via(buffer)
        .map(Excerpt::getElement)
        .map(Element::getValue)
        .fold(new ArrayList<Integer>(), (ArrayList<Integer> a, Integer i) -> {
          a.add(i);
          return a;
            })
        .toMat(Sink.cancelled(), Keep.left())
        .run(getMaterializer())
        .expectCancellation();

  }

  @Test
  void exceptionalSource(ChronicleQueue queue, ActorSystem system) {

    Pair<TestPublisher.Probe<Integer>, TestSubscriber.Probe<Integer>> probes =
        createProbes(queue, system);
    TestPublisher.Probe<Integer> sourceProbe = probes.first();
    TestSubscriber.Probe<Integer> sinkProbe = probes.second();

    RuntimeException expectedError = new RuntimeException("A testing exception");

    sourceProbe.sendError(expectedError);


    Throwable foundError = sinkProbe.request(10)
        .expectError();

    Assertions.assertEquals("A testing exception", foundError.getMessage(),
        "The expected exception msg is different than expected");

  }

  Pair<TestPublisher.Probe<Integer>, TestSubscriber.Probe<Integer>> createProbes(
      ChronicleQueue queue, ActorSystem system) {
    ChronicleQueueBuffer<Element> buffer = createBuffer(queue);

    return TestSource.<Integer>probe(system)
        .map(Element::new)
        .via(buffer)
        .map(Excerpt::getElement)
        .map(Element::getValue)
        .toMat(TestSink.probe(system), Keep.both())
        .run(getMaterializer());
  }

}
