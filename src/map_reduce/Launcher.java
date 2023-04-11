package map_reduce;

import static map_reduce.fake_net.Connection.getConnection;
import map_reduce.logging.Logger;
import map_reduce.work.Master;
import map_reduce.work.Worker;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Launcher {

    private static final String filename =
            "word count";


    public static void main(String[] args)
            throws IOException, InterruptedException {

        var master  = new Master();
        var workers = new LinkedList<Worker>();
        var logger = new Logger("Root");

        logger.info("create mappers");
        for (int i = 0; i < 100; i++) {
            var worker = new Worker();
            workers.add(worker);
            getConnection().addWorkerAsMapper(worker);
        }

        logger.info("create reducers");
        for (int i = 0; i < 50; i++) {
            var worker = new Worker();
            workers.add(worker);
            getConnection().addWorkerAsReducer(worker);
        }

        logger.info("read file");
        var raw = Files
                .readAllBytes(Path.of(filename + ".txt"));
        var text = new String(raw, UTF_8);


        logger.info("submit tasks to master service");
        master.submit(text);

        logger.info("launch master service");
        var pro = master.launch();

        logger.info("waiting result");
        var ret = getConnection().result().take();

        try(var fos = new FileOutputStream(filename + "reduce");
            var oos = new ObjectOutputStream(fos)){

            oos.writeObject(ret);
        }
        pro.cancel(true);

        logger.info("shutdown services");
        workers.forEach(Worker::destroy);
        master.shutdown();
    }
}
