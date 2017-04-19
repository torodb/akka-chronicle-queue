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
import com.torodb.testing.core.junit5.SimplifiedParameterResolver;
import org.junit.jupiter.api.extension.ExtensionContext;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.Optional;

/**
 *
 */
public class ActorSystemExtension extends SimplifiedParameterResolver<ActorSystem> {

  @Override
  protected Class<ActorSystem> getParameterClass() {
    return ActorSystem.class;
  }

  @Override
  protected ActorSystem createParamValue(ExtensionContext context) {
    return ActorSystem.create("testing");
  }

  @Override
  protected boolean cleanAfterTest(ExtensionContext context) {
    return false;
  }

  @Override
  protected void cleanCallback(Optional<ActorSystem> param) {
    param.ifPresent(actorSystem -> {
      try {
        Await.result(actorSystem.terminate(), Duration.Inf());
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    });
  }

}
