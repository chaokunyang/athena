package com.timeyang.athena.task.exec;

import com.timeyang.athena.utill.Asserts;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Log index for fast seek for laege file
 * <p>
 *     one index use 8 bytes.
 *     up to 1000000 indexes.
 *     one index every 100 line, 100000000 lines in total.
 *     if every 10 lines use up 1 kb, then it can index file up to 10G
 * </p>
 * Not thread-save
 * @author https://github.com/chaokunyang
 */
public class LogIndex {
    public static final int INDEX_PER_LINES = 100;
    private ByteBuffer byteBuffer;
    private AtomicBoolean flipped = new AtomicBoolean();

    public LogIndex() {
        // 80M index memory, 10 * 1024 * 1024 file offsets。
        byteBuffer = ByteBuffer.allocateDirect(80 * 1024 * 1024);
    }

    /**
     * index every{@code INDEX_PER_LINES}. index must be continuous every {@code INDEX_PER_LINES}
     * @param lineNumber line number
     * @param filePointer file byte offset
     * @return ture if indexed
     */
    public boolean put(int lineNumber, long filePointer) {
        Asserts.check(lineNumber > 0, "line number must be greater than 0");
        lineNumber = lineNumber - 1; // internal line number starts form 0

        int indexedOffset = lineNumber / INDEX_PER_LINES * 8;
        if (indexedOffset == byteBuffer.position()) {
            byteBuffer.putLong(filePointer);
            return true;
        }

        return false;
    }

    public Optional<Index> getRecentIndex(int lineNumber) {
        Asserts.check(lineNumber > 0, "line number must be greater than 0");
        lineNumber = lineNumber - 1; // internal line number starts form 0

        int offset = lineNumber / INDEX_PER_LINES * 8;

        if (offset >= byteBuffer.position()) { // current line doesn't be indexed, get last index
            if (byteBuffer.position() >= 8) {
                long filePtr = byteBuffer.getLong(byteBuffer.position() - 8);
                int indexedLineNumber = (byteBuffer.position() / 8 - 1) * INDEX_PER_LINES;
                Index index = new Index(indexedLineNumber, filePtr);
                return Optional.of(index);
            }
            return Optional.empty();
        } else { // current line is in index range
            int indexedLineNumber = lineNumber / INDEX_PER_LINES * INDEX_PER_LINES + 1; // line number starts from 1
            Index index = new Index(indexedLineNumber, byteBuffer.getLong(offset));
            return Optional.of(index);
        }
    }

    public List<Index> getIndices() {
        List<Index> indices = new ArrayList<>(byteBuffer.position() / 8);
        for (int i = 0; i < byteBuffer.position() / 8; i++) {
            int indexedLineNumber = i * INDEX_PER_LINES + 1; // line number starts from 1
            Index index = new Index(indexedLineNumber, byteBuffer.getLong(i * 8));
            indices.add(index);
        }

        return indices;
    }

    public static class Index {
        private final int lineNumber;
        private final long filePointer;

        Index(int lineNumber, long filePointer) {
            this.lineNumber = lineNumber;
            this.filePointer = filePointer;
        }

        public int getLineNumber() {
            return lineNumber;
        }

        public long getFilePointer() {
            return filePointer;
        }

        @Override
        public String toString() {
            return "Index{" +
                    "lineNumber=" + lineNumber +
                    ", filePointer=" + filePointer +
                    '}';
        }
    }

}
