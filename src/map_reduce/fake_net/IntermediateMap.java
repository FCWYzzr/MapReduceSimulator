package map_reduce.fake_net;

import java.util.Map;
import java.util.Queue;

public record IntermediateMap(
        Map<String, Integer> map1,
        Map<String, Integer> map2
) {
    public static IntermediateMap
        take(Queue<Map<String, Integer>> queue){

        return new IntermediateMap(
                queue.poll(),
                queue.poll()
        );
    }
}
