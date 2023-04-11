package map_reduce.work;

import map_reduce.fake_net.IntermediateMap;
import map_reduce.fake_net.MappingChannel;
import map_reduce.fake_net.ReducingChannel;
import map_reduce.fake_net.Connection;
import map_reduce.logging.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;
import static map_reduce.fake_net.Connection.getConnection;

public class Master{
    private final Map<Worker, String> mappingTaskRecord;
    private final Map<Worker, IntermediateMap> reducingTaskRecord;

    private final Queue<String> mappingTasks;
    private final Queue<Map<String, Integer>> reducingTasks;
    private final ScheduledThreadPoolExecutor executor;

    private final AtomicInteger mappingCounter;
    private final AtomicInteger reducingCounter;
    private int taskSize;

    private final Logger logger;

    ScheduledFuture<?> mapping;
    ScheduledFuture<?> reducing;


    public Master() {
        mappingTaskRecord = new HashMap<>();
        reducingTaskRecord = new HashMap<>();

        mappingTasks = new LinkedList<>();
        reducingTasks = new LinkedList<>();

        executor = new ScheduledThreadPoolExecutor(6);
        executor.setRemoveOnCancelPolicy(true);

        logger = new Logger("Master");
        mappingCounter = new AtomicInteger(0);
        reducingCounter = new AtomicInteger(0);


        mapping = null;
    }

    public void submit(String text) {

        var tasks = Arrays.asList(text.split(" +"));

        logger.info("task get, %d in total".formatted(tasks.size()));
        taskSize = tasks.size();


        mappingTasks.addAll(
                tasks
        );

        executor.execute(()->{
            while(!mappingTasks.isEmpty() || reducingTasks.size() != 1
                ||!mappingTaskRecord.isEmpty() || !reducingTaskRecord.isEmpty()) {
                try {
                    //noinspection BusyWait
                    sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
            }
            logger.info("task done");

            var ret = reducingTasks.poll();

            assert ret != null;

            //noinspection ResultOfMethodCallIgnored
            getConnection().result().offer(ret);
        });
    }


    public ScheduledFuture<?> launch(){

        logger.info("launch tasks");

        logger.debug("launch handleMappingTask");
        startMapping();

        logger.debug("launch handleReducingTask");
        startReducing();

        logger.debug("launch syncWithWorker");
        executor.scheduleWithFixedDelay(
                this::syncWithWorker
                ,0,
                10,
                TimeUnit.MILLISECONDS
        );

        logger.debug("launch clearDeadReducer");
        executor.scheduleWithFixedDelay(
                this::clearDeadReducer
                ,0,
                5,
                TimeUnit.SECONDS
        );

        logger.debug("launch clearDeadMapper");
        executor.scheduleWithFixedDelay(
                this::clearDeadMapper
                ,0,
                5,
                TimeUnit.SECONDS
        );


        return executor.scheduleWithFixedDelay(()->
            logger.progress(
                    (double) mappingCounter.get() / taskSize,
                    (double) reducingCounter.get() / taskSize
            ),0, 500, TimeUnit.MILLISECONDS);
    }

    private Worker getAvailableMapper(){
        var worker = findMapper();

        while (worker.isEmpty())
            worker = findMapper();

        return worker.get();
    }

    private Worker getAvailableReducer(){
        var worker = findReducer();

        while (worker.isEmpty())
            worker = findMapper();


        return worker.get();
    }

    private Set<Worker> mapperCheckAlive(){

        logger.debug("checking mappers");

        var dead = new HashSet<Worker>();

        getConnection()
                .mappingChannels()

                .entrySet()
                .stream()

                .filter(entry -> entry.getKey().isDead())

                .forEach(entry ->
                    dead.add(entry.getKey())
                );
        if (!dead.isEmpty())
            logger.info("clear %d dead mappers".formatted(dead.size()));

        return dead;
    }
    private Set<Worker> reducerCheckAlive(){
        var dead = new HashSet<Worker>();

        logger.debug("checking reducers");

        getConnection()
                .reducingChannels()

                .entrySet()
                .stream()

                .filter(entry -> entry.getKey().isDead())

                .forEach(entry ->
                        dead.add(entry.getKey())
                );
        if (!dead.isEmpty())
            logger.info("clear %d dead reducers".formatted(dead.size()));

        return dead;
    }

    private List<String> deleteMappers(Set<Worker> mappers){
        if (mappers.isEmpty())
            return List.of();

        var result = new LinkedList<String>();

        var pairs =
                getConnection().mappingChannels();

        synchronized (mappingTaskRecord) {
            mappers.forEach(worker -> {
                pairs.remove(worker);

                var notDone = mappingTaskRecord.remove(worker);

                result.add(notDone);
            });
        }

        logger.info("recycled %d mapping tasks".formatted(result.size()));

        return result;
    }
    private List<IntermediateMap> deleteReducer(Set<Worker> reducer){
        if (reducer.isEmpty())
            return List.of();

        var result = new LinkedList<IntermediateMap>();

        var pairs =
                getConnection().mappingChannels();

        synchronized (reducingTaskRecord) {
            reducer.forEach(worker -> {
                pairs.remove(worker);
                var notDone = reducingTaskRecord.remove(worker);
                result.add(notDone);
            });
        }

        logger.info("recycled %d reducing tasks".formatted(result.size()));

        return result;
    }

    private void redistributeMapping(List<String> tasks){
        this.mappingTasks.addAll(tasks);
        startMapping();
    }

    private void redistributeReducing(List<IntermediateMap> tasks){
        tasks.forEach(task-> {
            reducingTasks.add(task.map1());
            reducingTasks.add(task.map2());
        });
    }

    private void handleMappingTask(){
        if (mappingTasks.isEmpty() && mappingTaskRecord.isEmpty()) {
            stopMapping();
            return;
        }

        if (mappingTasks.isEmpty())
            return;

        String task;

        task = mappingTasks.poll();
        logger.debug("mapping get");

        var worker = getAvailableMapper();
        logger.debug("mapper found");

        Connection.submit(worker, task);
        logger.debug("mapping submitted");

        synchronized (mappingTaskRecord) {
            mappingTaskRecord.put(worker, task);
            logger.debug("record mapping:" + task);
        }
    }

    private void handleReducingTask(){
        if (reducingTaskRecord.isEmpty()
                && reducingTasks.size() == 1) {
            stopReducing();
            return;
        }


        if (reducingTasks.size() < 2)
            return;

        IntermediateMap task;

        task = IntermediateMap.take(
                reducingTasks
        );
        logger.debug("reducing get");

        var worker = getAvailableReducer();
        logger.debug("reducer found");

        Connection.submit(worker, task);
        logger.debug("reducing submitted");

        synchronized (reducingTaskRecord) {
            reducingTaskRecord.put(worker, task);
            logger.debug("record reducing:" + task);
        }

    }

    private void syncWithWorker(){
        getConnection()
                .mappingChannels()
                .forEach(this::syncMapping);

        getConnection()
                .reducingChannels()
                .forEach(this::syncReducing);
    }

    private void startMapping(){
        if (mapping != null)
            return;

        logger.info("start mapping");
        mapping = executor.scheduleWithFixedDelay(
                this::handleMappingTask,
                0,
                10,
                TimeUnit.MILLISECONDS
        );
    }

    private void startReducing(){
        if (reducing != null)
            return;

        logger.info("start reducing");
        reducing = executor.scheduleWithFixedDelay(
                this::handleReducingTask,
                0,
                10,
                TimeUnit.MILLISECONDS
        );
    }

    private void stopMapping(){
        mapping.cancel(false);
        mapping = null;
        logger.info("no more mapping");

    }


    private void stopReducing(){
        reducing.cancel(false);
        reducing = null;
        logger.info("no more reducing");
    }

    private Optional<Worker> findMapper(){
        synchronized (mappingTaskRecord) {
            return getConnection().getMappers()
                    .stream()
                    .filter(worker -> !mappingTaskRecord.containsKey(worker))
                    .findFirst();
        }
    }

    private Optional<Worker> findReducer(){
        synchronized (reducingTaskRecord) {
            return getConnection().getReducers()
                    .stream()
                    .filter(worker -> !reducingTaskRecord.containsKey(worker))
                    .findFirst();
        }

    }

    private void clearDeadReducer(){
        var deadWorkers =
                reducerCheckAlive();
        var notDone =
                deleteReducer(deadWorkers);
        redistributeReducing(notDone);
    }

    private void clearDeadMapper(){
        var deadWorkers =
                mapperCheckAlive();
        var notDone =
                deleteMappers(deadWorkers);
        redistributeMapping(notDone);
    }

    public void shutdown(){
        executor.shutdownNow();
    }

    private void syncMapping(Worker worker, MappingChannel channel) {
        var results = channel.results();
        var result = results.poll();

        if (result == null)
            return;


        reducingTasks.offer(result);
        mappingCounter.incrementAndGet();

        synchronized (mappingTaskRecord) {
            mappingTaskRecord.remove(worker);
            logger.debug("set mapper available");
        }

        startReducing();
    }

    private void syncReducing(Worker worker, ReducingChannel channel) {
        var results = channel.results();
        var result = results.poll();

        if (result == null)
            return;


        reducingTasks.offer(result);
        reducingCounter.incrementAndGet();

        synchronized (reducingTaskRecord) {
            reducingTaskRecord.remove(worker);
            logger.debug("set reducer available");
        }
    }
}
