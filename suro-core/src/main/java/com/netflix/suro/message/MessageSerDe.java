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

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;

/**
 * Message itself should be (de)serialized. This serde is mainly used to persist
 * messages to somewhere including file based queue
 *
 * @author jbae
 */
public class MessageSerDe implements SerDe<Message> {
    private final static Logger log = LoggerFactory.getLogger(MessageSerDe.class);

    private ThreadLocal<ByteArrayOutputStream> outputStream =
            new ThreadLocal<ByteArrayOutputStream>() {
                @Override
                protected ByteArrayOutputStream initialValue() {
                    return new ByteArrayOutputStream();
                }

                @Override
                public ByteArrayOutputStream get() {
                    ByteArrayOutputStream b = super.get();
                    b.reset();
                    return b;
                }
            };

    @Override
    public Message deserialize(byte[] payload) {
        try {
            DataInput dataInput = ByteStreams.newDataInput(payload);
            Class<? extends Message> clazz = Message.classMap.get(dataInput.readByte());
            Message msg = clazz.newInstance();
            msg.readFields(dataInput);
            return msg;
        } catch (Exception e) {
            log.error("Exception on deserialize: " + e.getMessage(), e);
            return new Message();
        }
    }

    @Override
    public byte[] serialize(Message payload) {
        try {
            ByteArrayDataOutput out = new ByteArrayDataOutputStream(outputStream.get());
            out.writeByte(Message.classMap.inverse().get(payload.getClass()));
            payload.write(out);
            return out.toByteArray();
        } catch (IOException e) {
            log.error("Exception on serialize: " + e.getMessage(), e);
            return new byte[]{};
        }
    }

    @Override
    public String toString(byte[] payload) {
        return deserialize(payload).toString();
    }
}
