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

import jdk.nashorn.internal.ir.annotations.Immutable;

/**
 *
 */
@Immutable
public class Excerpt<T> {

  private final long index;
  private final T element;

  public Excerpt(long index, T element) {
    this.index = index;
    this.element = element;
  }

  public long getIndex() {
    return index;
  }

  public T getElement() {
    return element;
  }

}
