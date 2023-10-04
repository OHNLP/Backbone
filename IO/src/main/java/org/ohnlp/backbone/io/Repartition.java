package org.ohnlp.backbone.io;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.concurrent.ThreadLocalRandom;

public class Repartition<T> extends PTransform<PCollection<T>, PCollection<T>> {

    private Repartition() {
    }

    public static <T> Repartition<T> of() {
        return new Repartition<>();
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        return input
                .apply(ParDo.of(new DoFn<T, KV<Integer, T>>() {
                    @ProcessElement
                    public void process(ProcessContext pc) {
                        pc.output(KV.of(ThreadLocalRandom.current().nextInt(), pc.element()));
                    }
                }))
                .apply(GroupByKey.<Integer, T>create())
                .apply(ParDo.of(new DoFn<KV<Integer, Iterable<T>>, T>() {
                    @ProcessElement
                    public void process(ProcessContext pc) {
                        for (T element : pc.element().getValue()) {
                            pc.output(element);
                        }
                    }
                }));
    }
}
