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
import akka.stream.Attributes;
import akka.stream.Inlet;
import akka.stream.SinkShape;
import akka.stream.stage.GraphStageLogic;
import akka.stream.stage.GraphStageWithMaterializedValue;
import akka.stream.stage.InHandler;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import scala.Tuple2;

import java.util.concurrent.CompletableFuture;

/**
 *
 */
public class ChronicleQueueSink<T> extends 
    GraphStageWithMaterializedValue<SinkShape<T>, CompletableFuture<NotUsed>> {

  private final Inlet<T> in = Inlet.create(ChronicleQueueSink.class.getSimpleName() + ".in");
  private final SinkShape<T> shape = SinkShape.of(in);
  private final ChronicleQueue queue;
  private final WriteMarshaller<T> writer;

  ChronicleQueueSink(ChronicleQueue queue, WriteMarshaller<T> writer) {
    this.queue = queue;
    this.writer = writer;
  }

  ChronicleQueue getQueue() {
    return queue;
  }

  @Override
  public SinkShape<T> shape() {
    return shape;
  }

  @Override
  public Tuple2<GraphStageLogic, CompletableFuture<NotUsed>> createLogicAndMaterializedValue(
      Attributes inheritedAttributes) throws Exception {

    CompletableFuture<NotUsed> materializedValue = new CompletableFuture<>();
    return new Tuple2<>(createLogic(materializedValue), materializedValue);
  }

  protected Logic createLogic(CompletableFuture<NotUsed> materializedValue) {
    return new Logic(materializedValue);
  }

  protected class Logic extends GraphStageLogic {

    private final ExcerptAppender appender;
    private final CompletableFuture<NotUsed> materializedValue;
    private Throwable errorFound = null;

    public Logic(CompletableFuture<NotUsed> materializedValue) {
      super(shape());
      this.materializedValue = materializedValue;
      
      this.appender = queue.acquireAppender();

      setHandler(in, new InHandler() {
        @Override
        public void onPush() throws Exception {
          T elem = grab(in);

          try {
            appender.writeDocument(wire -> writer.accept(wire, elem));
            pull(in);
            
          } catch (Exception ex) {
            errorFound = ex;
            failStage(ex);
          }
        }

        @Override
        public void onUpstreamFailure(Throwable ex) throws Exception {
          materializedValue.completeExceptionally(ex);
          InHandler.super.onUpstreamFailure(ex);
        }

        @Override
        public void onUpstreamFinish() throws Exception {
          materializedValue.complete(NotUsed.getInstance());
          InHandler.super.onUpstreamFinish();
        }
      });
    }

    @Override
    public void preStart() throws Exception {
      pull(in);
    }

    @Override
    public void postStop() throws Exception {
      if (errorFound != null) {
        materializedValue.completeExceptionally(errorFound);
      } else {
        materializedValue.complete(NotUsed.getInstance());
      }
    }

  }

}
