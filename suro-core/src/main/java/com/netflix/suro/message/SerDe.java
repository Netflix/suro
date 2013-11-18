/*
 * Copyright 2013 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.suro.message;

/**
 * SerDe(Serialization/Deserialization) is necessary for converting POJO to
 * byte[] vice versa. serialize() receives POJO and returns byte[], deserialize
 * does reverse. For Java String, StringSerDe is implemented.
 *
 * @author jbae
 *
 * @param <T>
 */
public interface SerDe<T> {
    T deserialize(byte[] payload);

    byte[] serialize(T payload);

    String toString(byte[] payload);
}
