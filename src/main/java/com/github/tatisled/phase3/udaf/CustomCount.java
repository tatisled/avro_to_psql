package com.github.tatisled.phase3.udaf;

import org.apache.beam.sdk.transforms.Combine;

import java.util.Iterator;

/**
 * UDAF(CombineFn) to implement count distinct
 * todo test (notice that it returns incremented counter which is wrong)
 */
public class CustomCount extends Combine.CombineFn<Object, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long addInput(Long accumulator, Object input) {
        return accumulator + 1;
    }

    @Override
    public Long mergeAccumulators(Iterable<Long> accumulators) {
        long v = 0L;
        Iterator<Long> ite = accumulators.iterator();
        while (ite.hasNext()) {
            v += ite.next();
        }
        return v;
    }

    @Override
    public Long extractOutput(Long accumulator) {
        return accumulator;
    }
}
