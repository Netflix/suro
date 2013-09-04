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

package com.netflix.suro.message.serde;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.netflix.suro.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

public class MessageSerDe implements SerDe<Message> {
    static Logger log = LoggerFactory.getLogger(MessageSerDe.class);

    protected ThreadLocal<ByteArrayOutputStream> outputStream =
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
            Message msg = new Message();
            msg.readFields(ByteStreams.newDataInput(payload));
            return msg;
        } catch (IOException e) {
            log.error("Exception on deserialize: " + e.getMessage(), e);
            return new Message();
        }
    }

    @Override
    public byte[] serialize(Message payload) {
        try {
            ByteArrayDataOutput out = new ByteArrayDataOutputStream(outputStream.get());
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

    class ByteArrayDataOutputStream
            implements ByteArrayDataOutput {

        final DataOutput output;
        final ByteArrayOutputStream byteArrayOutputSteam;

        ByteArrayDataOutputStream(ByteArrayOutputStream byteArrayOutputSteam) {
            this.byteArrayOutputSteam = byteArrayOutputSteam;
            output = new DataOutputStream(byteArrayOutputSteam);
        }

        @Override public void write(int b) {
            try {
                output.write(b);
            } catch (IOException impossible) {
                throw new AssertionError(impossible);
            }
        }

        @Override public void write(byte[] b) {
            try {
                output.write(b);
            } catch (IOException impossible) {
                throw new AssertionError(impossible);
            }
        }

        @Override public void write(byte[] b, int off, int len) {
            try {
                output.write(b, off, len);
            } catch (IOException impossible) {
                throw new AssertionError(impossible);
            }
        }

        @Override public void writeBoolean(boolean v) {
            try {
                output.writeBoolean(v);
            } catch (IOException impossible) {
                throw new AssertionError(impossible);
            }
        }

        @Override public void writeByte(int v) {
            try {
                output.writeByte(v);
            } catch (IOException impossible) {
                throw new AssertionError(impossible);
            }
        }

        @Override public void writeBytes(String s) {
            try {
                output.writeBytes(s);
            } catch (IOException impossible) {
                throw new AssertionError(impossible);
            }
        }

        @Override public void writeChar(int v) {
            try {
                output.writeChar(v);
            } catch (IOException impossible) {
                throw new AssertionError(impossible);
            }
        }

        @Override public void writeChars(String s) {
            try {
                output.writeChars(s);
            } catch (IOException impossible) {
                throw new AssertionError(impossible);
            }
        }

        @Override public void writeDouble(double v) {
            try {
                output.writeDouble(v);
            } catch (IOException impossible) {
                throw new AssertionError(impossible);
            }
        }

        @Override public void writeFloat(float v) {
            try {
                output.writeFloat(v);
            } catch (IOException impossible) {
                throw new AssertionError(impossible);
            }
        }

        @Override public void writeInt(int v) {
            try {
                output.writeInt(v);
            } catch (IOException impossible) {
                throw new AssertionError(impossible);
            }
        }

        @Override public void writeLong(long v) {
            try {
                output.writeLong(v);
            } catch (IOException impossible) {
                throw new AssertionError(impossible);
            }
        }

        @Override public void writeShort(int v) {
            try {
                output.writeShort(v);
            } catch (IOException impossible) {
                throw new AssertionError(impossible);
            }
        }

        @Override public void writeUTF(String s) {
            try {
                output.writeUTF(s);
            } catch (IOException impossible) {
                throw new AssertionError(impossible);
            }
        }

        @Override public byte[] toByteArray() {
            return byteArrayOutputSteam.toByteArray();
        }
    }
}
