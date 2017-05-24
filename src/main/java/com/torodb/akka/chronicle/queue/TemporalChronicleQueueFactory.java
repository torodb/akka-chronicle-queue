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

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 *
 */
public class TemporalChronicleQueueFactory {

  private TemporalChronicleQueueFactory() {
  }

  public static SingleChronicleQueueBuilder temporalQueueBuilder() {
    Path path;
    try {
      path = Files.createTempDirectory("cq-akka-test");
      deleteOnClose(path);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    return SingleChronicleQueueBuilder.binary(path);
  }

  public static ChronicleQueue createTemporalQueue() {
    return temporalQueueBuilder().build();
  }

  @SuppressWarnings("checkstyle:EmptyCatchBlock")
  private static void deleteFolder(Path path) {
    try {
      Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (IOException ignored) {
    }
  }

  private static void deleteOnClose(Path path) {
    Runnable runnable = () -> deleteFolder(path);
    Runtime.getRuntime().addShutdownHook(new Thread(runnable, "deleteOnClose-" + path));
  }

}
