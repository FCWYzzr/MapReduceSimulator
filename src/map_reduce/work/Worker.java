package map_reduce.work;


import map_reduce.fake_net.IntermediateMap;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static map_reduce.fake_net.Connection.getConnection;


public class Worker{
    private static int id = 0;

    private final ScheduledThreadPoolExecutor executor;
    private boolean alive;
    private final String name;

    public Worker() {
        name = "Worker-%d".formatted(id++);

        executor = new ScheduledThreadPoolExecutor(1,
                run -> {
                    var th = new Thread(run);

                    th.setDaemon(true);
                    return th;
                }
        );
    }

    public void asMapper(){
        final var channel =
                getConnection().getMapperChannel(this);

        executor.scheduleWithFixedDelay(()->{try{

                var task = channel.tasks().take();


                channel.results().offer(
                        mapping(task)
                );

        }catch (InterruptedException ignored) {}
        }, 0, 10, TimeUnit.MILLISECONDS);

        alive = true;
    }

    public void asReducer(){
        final var channel =
                getConnection().getReducerChannel(this);

        executor.scheduleWithFixedDelay(()->{try{

            var task = channel.tasks().take();


            channel.results().offer(
                    reduce(task)
            );

        }catch (InterruptedException ignored) {}
        }, 0, 10, TimeUnit.MILLISECONDS);

        alive = true;
    }

    public Map<String, Integer> mapping(String word){
        return Map.of(word, 1);
    }

    public Map<String, Integer> reduce(IntermediateMap task){
        Map<String, Integer> ret;

        ret = new HashMap<>(task.map1());

        task.map2().forEach((k, v) -> {
            if (ret.containsKey(k))
                ret.put(k, ret.get(k) + v);
            else
                ret.put(k, v);
        });

        return ret;
    }

    public void destroy(){
        executor.shutdownNow();
        alive = false;
    }

    public boolean isDead() {
        return !alive;
    }

    @Override
    public String toString() {
        return name;
    }
}
