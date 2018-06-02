package com.timeyang.athena.task.exec;

import com.timeyang.athena.AthenaException;
import com.timeyang.athena.task.exec.LogIndex.Index;
import io.netty.channel.FileRegion;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Log Query
 * <p>Because log is query by lines, so it's needed to loaded to memory, and we can't use zero-copy such as @link FileRegion}</p>
 *
 * @author https://github.com/chaokunyang
 */
public class LogInspection {
    private RandomAccessFile file;
    private LogIndex logIndex;

    LogInspection(String filePath) {
        try {
            file = new RandomAccessFile(filePath, "r");
            logIndex = new LogIndex();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            String msg = String.format("Can't open task log file [%s]", filePath);
            throw new AthenaException(msg, e);
        }
    }

    /**
     * Get log lines. use synchronized to avoid concurrent operating LogIndex and log file.
     *
     * @param lineNumber start line (1 started)
     * @param rows       line number
     * @return log lines
     */
    public synchronized List<String> getLines(int lineNumber, int rows) {
        try {
            Optional<Index> optionalIndex = logIndex.getRecentIndex(lineNumber);
            if (optionalIndex.isPresent()) {
                Index index = optionalIndex.get();
                file.seek(index.getFilePointer());
                int indexedLineNumber = index.getLineNumber();
                for (int i = 1; i < lineNumber - indexedLineNumber; i++) {
                    logIndex.put(indexedLineNumber + i, file.getFilePointer());
                    if (file.readLine() == null)
                        return new ArrayList<>();
                }
            } else {
                for (int i = 1; i < lineNumber; i++) {
                    logIndex.put(i, file.getFilePointer());
                    if (file.readLine() == null)
                        return new ArrayList<>();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return new ArrayList<>();
        }

        List<String> lines = new ArrayList<>(rows);
        try {
            for (int i = 1; i <= rows; i++) {
                logIndex.put(lineNumber + i, file.getFilePointer());
                String line = file.readLine();
                if (line == null)
                    return lines;
                lines.add(line);
            }

            return lines;
        } catch (IOException e) {
            e.printStackTrace();
            return lines;
        }
    }

    public LogIndex getLogIndex() {
        return logIndex;
    }

    public void close() {
        try {
            file.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
