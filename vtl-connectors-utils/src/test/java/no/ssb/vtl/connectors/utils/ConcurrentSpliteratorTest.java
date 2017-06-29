package no.ssb.vtl.connectors.utils;

/*-
 * ========================LICENSE_START=================================
 * Java VTL Utility connectors
 * %%
 * Copyright (C) 2017 Statistics Norway and contributors
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import org.assertj.core.util.Lists;

import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class ConcurrentSpliteratorTest {

    // TODO
    //@Test
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
