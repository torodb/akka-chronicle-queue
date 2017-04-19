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
import akka.stream.stage.GraphStageLogic;
import net.openhft.chronicle.queue.ChronicleQueue;
import scala.concurrent.duration.FiniteDuration;

import java.util.function.Supplier;

/**
 * A {@link ChronicleQueueSource} that manages the lifecycle of the {@link ChronicleQueue}, so
 * when {@link #createLogic(akka.stream.Attributes) logic associated with this object} is stopped,
 * the queue is closed.
 */
class AutoManagedChronicleQueueSource<T> extends ChronicleQueueSource<T> {

  AutoManagedChronicleQueueSource(
      Supplier<ChronicleQueue> queueSupplier,
      ReadMarshaller<T> reader,
      Supplier<FiniteDuration> pollingIntervalSupplier) {
    super(queueSupplier.get(), reader, pollingIntervalSupplier);
  }

  @Override
  public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
    return new Logic() {
      @Override
      public void postStop() throws Exception {
        getQueue().close();
      }

    };
  }

}
