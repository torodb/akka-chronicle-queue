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

import com.torodb.testing.core.junit5.SimplifiedParameterResolver;
import net.openhft.chronicle.queue.ChronicleQueue;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Optional;

/**
 *
 */
public class ChronicleQueueExtension extends SimplifiedParameterResolver<ChronicleQueue> {

  @Override
  protected Class<ChronicleQueue> getParameterClass() {
    return ChronicleQueue.class;
  }

  @Override
  protected ChronicleQueue createParamValue(ExtensionContext context) {
    return TemporalChronicleQueueFactory.createTemporalQueue();
  }

  @Override
  protected boolean cleanAfterTest(ExtensionContext context) {
    return true;
  }

  @Override
  protected void cleanCallback(Optional<ChronicleQueue> param) {
    param.ifPresent(queue -> {
      queue.close();
    });
  }
}
