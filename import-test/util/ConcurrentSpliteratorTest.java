package no.ssb.vtl.tools.sandbox.connector.util;

import org.assertj.core.util.Lists;
import org.junit.Test;

import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by hadrien on 14/06/2017.
 */
public class ConcurrentSpliteratorTest {

    @Test
    public void test() throws Exception {

        List<Integer> range = IntStream.rangeClosed(1, 100)
                .boxed().collect(Collectors.toList());

        ConcurrentSpliterator<Integer> concurrentSpliterator;
        concurrentSpliterator = ConcurrentSpliterator.create(
                50,
                range::spliterator
        );

        List<List<Integer>> lists = Lists.newArrayList();

        int threads = 2;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        for (int i = 0; i < threads; i++) {
            Thread thread = new Thread(() -> {
                List<Integer> list = Lists.newArrayList();
                Spliterator<Integer> s = concurrentSpliterator.get();
                try {
                    s.forEachRemaining(list::add);
//                    while (s.tryAdvance(integer -> {
//                        list.add(integer);
//                        System.out.printf(
//                                "%s: %s\n",
//                                Thread.currentThread(),
//                                integer
//                        );
//                    }));
                } finally {
                    lists.add(list);
                    try {
                        barrier.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                }

            });
            thread.start();
        }

        barrier.await();
        for (List<Integer> list : lists) {
            assertThat(list).containsExactlyElementsOf(range);
        }

    }
}
