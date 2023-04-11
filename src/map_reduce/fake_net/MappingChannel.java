package map_reduce.fake_net;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public record MappingChannel(
        BlockingQueue<String> tasks,
        Queue<Map<String, Integer>> results
) {
    public MappingChannel() {
        this(
                new LinkedBlockingQueue<>(),
                new LinkedList<>()
        );
    }
}
