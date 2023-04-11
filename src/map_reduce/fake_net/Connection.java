package map_reduce.fake_net;

import map_reduce.work.Worker;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public record Connection(
        Map<Worker, MappingChannel> mappingChannels,
        Map<Worker, ReducingChannel> reducingChannels,
        BlockingQueue<Map<String, Integer>> result
) {

    public static final Connection instance;

    public static Connection getConnection(){
        return instance;
    }

    static {
        instance = new Connection(
                new HashMap<>(),
                new HashMap<>(),
                new LinkedBlockingQueue<>()
        );
    }

    public Set<Worker> getMappers(){
        return mappingChannels.keySet();
    }

    public Set<Worker> getReducers(){
        return reducingChannels.keySet();
    }


    public MappingChannel getMapperChannel(Worker worker){
        return mappingChannels.get(worker);
    }

    public ReducingChannel getReducerChannel(Worker worker){
        return reducingChannels.get(worker);
    }


    public void addWorkerAsMapper(Worker worker) {
        mappingChannels.put(worker, new MappingChannel());
        worker.asMapper();
    }

    public void addWorkerAsReducer(Worker worker) {
        reducingChannels.put(worker, new ReducingChannel());
        worker.asReducer();
    }


    /** @noinspection ResultOfMethodCallIgnored*/
    public static void submit(Worker worker, String task){
        instance.mappingChannels.get(worker)
                .tasks().offer(task);
    }

    public static void submit(Worker worker, IntermediateMap task){
        //noinspection ResultOfMethodCallIgnored
        instance.reducingChannels.get(worker)
                .tasks().offer(task);
    }
}
