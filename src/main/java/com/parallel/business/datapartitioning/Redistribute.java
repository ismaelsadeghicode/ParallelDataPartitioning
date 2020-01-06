package com.parallel.business.datapartitioning;

import com.parallel.model.common.CPU;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Ismael Sadeghi, 2020-01-06 19:36
 */
public class Redistribute<T> {
    private List<CPU<T>> cpus;

    public Redistribute(List<CPU<T>> cpus) {
        this.cpus = cpus;
    }

    public List<CPU<T>> doRedistribute() {
        if (cpus == null || cpus.isEmpty()) {
            return new ArrayList<>();
        }
        int size = cpus.size();
        int max = 0;
        int min = Integer.MAX_VALUE;

        for (CPU<T> cpu : cpus) {
            int thismax = cpu.getBuffer().parallelStream().mapToInt(i -> (int) i).max().getAsInt();
            int thismin = cpu.getBuffer().parallelStream().mapToInt(i -> (int) i).min().getAsInt();

            max = (thismax > max) ? thismax : max;
            min = (thismin < min) ? thismin : min;
        }

        int step = (max - min) / size;

        for (CPU<T> cpu : cpus) {
            for (T data : cpu.getBuffer()) {
                boolean added = false;
                long mi = min;
                long mx = min + step;
                for (int i = 0; i < size; i++) {
                    if (((int) data) >= mi && ((int) data) < mx) {
                        cpus.get(i).getTemporary().add(data);
                        cpus.get(i).pluseActivityRedistribute();
                        cpus.get(i).setRedistributemax(mx);
                        cpus.get(i).setRedistributemin(mi);
                        added = true;
                        break;
                    }
                    mi = mx;
                    mx = mx + step;
                }
                // Last numbers
                if (!added) {
                    cpus.get(size - 1).getTemporary().add(data);
                    cpus.get(size - 1).pluseActivityRedistribute();
                    cpus.get(size - 1).setRedistributemax(mx);
                    cpus.get(size - 1).setRedistributemin(mi);
                }

            }
        }

        for (CPU<T> cpu : cpus) {
            cpu.setBuffer(cpu.getTemporary());
            cpu.setTemporary(new ArrayList<>());
        }

        return cpus;
    }

}

