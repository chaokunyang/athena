package com.timeyang.athena.task.message;

import com.timeyang.athena.AthenaException;
import com.timeyang.athena.task.exec.Task;
import com.timeyang.athena.task.message.TaskMessage.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;

import java.io.*;
import java.util.List;

/**
 * @author https://github.com/chaokunyang
 */
public class TaskMessageCodec extends ByteToMessageCodec<TaskMessage> {

    private static final byte OPCODE_OBJECT = 0x01;
    private static final byte OPCODE_TASK_SUBMIT = 0x02;
    private static final byte OPCODE_TASK_SUCCESS = 0x03;
    private static final byte OPCODE_KILL_TASK = 0x04;
    private static final byte OPCODE_HEARTBEAT = 0x05;

    private byte messageCode;
    private int messageLength;

    private int objectSize;

    @Override
    protected void encode(ChannelHandlerContext ctx, TaskMessage msg, ByteBuf out) throws Exception {
        if (msg instanceof ObjectMessage) {
            out.writeByte(OPCODE_OBJECT);

            byte[] bytes = serialize((ObjectMessage) msg);
            out.writeInt(bytes.length);
            out.writeBytes(bytes); // 数据
        }

        if (msg instanceof TaskSubmit) {
            out.writeByte(OPCODE_TASK_SUBMIT);

            byte[] bytes = serialize((TaskSubmit) msg);
            out.writeInt(bytes.length);
            out.writeBytes(bytes);
        }
        if (msg instanceof TaskSuccess) {
            out.writeByte(OPCODE_TASK_SUCCESS);

            Task task = ((TaskSuccess) msg).getTask();
            byte[] bytes = serialize(task);
            out.writeInt(bytes.length);
            out.writeBytes(bytes);
        }

        if (msg instanceof KillTask) {
            out.writeByte(OPCODE_KILL_TASK);
        }
        if (msg instanceof HeartBeat) {
            out.writeByte(OPCODE_HEARTBEAT);
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (messageCode == 0) {
            messageCode = in.readByte();
        }

        if (messageCode == OPCODE_OBJECT) {
            if (objectSize == 0) {
                if (in.readableBytes() < 4)
                    return;
                objectSize = in.readInt();
            } else {
                if (in.readableBytes() < objectSize)
                    return;
                byte[] obj = new byte[objectSize];
                in.readBytes(obj);

                out.add(deserialize(obj));

                // mark message read finished
                messageCode = 0;
                objectSize = 0;
            }
        }

        if (messageCode == OPCODE_TASK_SUBMIT ||
                messageCode == OPCODE_TASK_SUCCESS) {
            if (messageLength == 0) {
                if (in.readableBytes() < 4)
                    return;
                messageLength = in.readInt();
            } else {
                if (in.readableBytes() >= messageLength) {
                    byte[] bytes = new byte[messageLength];
                    in.readBytes(bytes);

                    if (messageCode == OPCODE_TASK_SUBMIT) {
                        TaskSubmit task = (TaskSubmit) deserialize(bytes);
                        out.add(task);
                    } else if (messageCode == OPCODE_TASK_SUCCESS) {
                        Task task = (Task) deserialize(bytes);
                        out.add(new TaskSuccess(task));
                    }

                    // mark message read finished
                    messageCode = 0;
                    messageLength = 0;
                }
            }
        }

        if (messageCode == OPCODE_KILL_TASK) {
            out.add(new KillTask());
            messageCode = 0; // mark message read finished
        }
        if (messageCode == OPCODE_HEARTBEAT) {
            out.add(new HeartBeat());
            messageCode = 0; // mark message read finished
        }
    }

    private static byte[] serialize(Serializable object) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput objectOutput;
        byte[] bytes;
        try {
            objectOutput = new ObjectOutputStream(bos);
            objectOutput.writeObject(object);
            objectOutput.flush();
            bytes = bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            throw new AthenaException("Can't serialize object " + object, e);
        } finally {
            try {
                bos.close();
            } catch (IOException ignored) {
            }
        }
        return bytes;
    }

    private static Object deserialize(byte[] bytes) {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput objectIn = null;
        try {
            objectIn = new ObjectInputStream(bis);
            return objectIn.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            throw new AthenaException("Can't deserialize bytes to Object", e);
        } finally {
            try {
                if (objectIn != null) {
                    objectIn.close();
                }
            } catch (IOException ignored) {
            }
        }
    }

}
