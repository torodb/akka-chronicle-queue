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
import net.openhft.chronicle.queue.ChronicleQueue;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A {@link ChronicleQueueSink} that automatically close the queue when it completes.
 */
class AutoManagedChronicleQueueSink<T> extends ChronicleQueueSink<T> {

  AutoManagedChronicleQueueSink(Supplier<ChronicleQueue> queueSupplier, WriteMarshaller<T> writer) {
    super(queueSupplier.get(), writer);
  }

  @Override
  protected Logic createLogic(CompletableFuture<NotUsed> materializedValue) {
    return new Logic(materializedValue) {
      @Override
      public void postStop() throws Exception {
        getQueue().close();
      }
    };
  }

}
