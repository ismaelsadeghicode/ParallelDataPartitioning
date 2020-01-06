package com.parallel.model.parallelmergall;

import java.util.List;

/**
 * @author Ismael Sadeghi, 2020-01-06 19:44
 */
public class Ram<T> {
    private int size;
    private List<T> buffer;

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public List<T> getBuffer() {
        return buffer;
    }

    public void setBuffer(List<T> buffer) {
        this.buffer = buffer;
    }
}
