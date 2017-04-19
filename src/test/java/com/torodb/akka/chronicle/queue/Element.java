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

import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

/**
 * A simple class used on tests.
 */
public class Element {

  private final int value;

  public Element(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static void write(WireOut wire, Element element) {
    wire.write("value").int32(element.getValue());
  }

  public static Element read(WireIn wire) {
    return new Element(wire.read("value").int32());
  }

  public static class ElementMarshaller implements Marshaller<Element> {

    @Override
    public void write(WireOut t, Element u) {
      t.getValueOut().int32(u.getValue());
    }

    @Override
    public Element read(WireIn t) {
      return new Element(t.read().int32());
    }

  }

}
