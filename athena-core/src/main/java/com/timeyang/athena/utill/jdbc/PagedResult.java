package com.timeyang.athena.utill.jdbc;

import java.util.List;

/**
 * Paged Result
 *
 * @author https://github.com/chaokunyang
 */
public class PagedResult<T> {
    public static final long DEFAULT_OFFSET = 0;
    public static final int DEFAULT_MAX_NO_OF_ROWS = 100;
    private int page;
    private int size;
    private int offset;
    private long totalSize;
    private List<T> elements;

    public PagedResult(List<T> elements, long totalSize, Page page) {
        this.elements = elements;
        this.totalSize = totalSize;
        this.page = page.getPage();
        this.offset = page.getOffset();
        this.size = elements.size();
    }

    public boolean hasMore() {
        return totalSize > page + size;
    }

    public boolean hasPrevious() {
        return page > 0 && totalSize > 0;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public int getPage() {
        return page;
    }

    public int getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }

    public List<T> getElements() {
        return elements;
    }

    @Override
    public String toString() {
        return "PagedResult{" +
                "page=" + page +
                ", offset=" + offset +
                ", size=" + size +
                ", totalSize=" + totalSize +
                ", elements=" + elements +
                '}';
    }
}
