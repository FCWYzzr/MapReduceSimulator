package map_reduce.logging;


import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Logger {
    public enum Level{
        DEBUG(0),
        INFO(1),
        WARNING(2),
        ERROR(3),
        FATAL(4);

        final int mask;

        Level(int mask) {
            this.mask = mask;
        }

        boolean lessThan(Level level){
            return this.mask < level.mask;
        }
    }
    private final String name;
    private final SimpleDateFormat format;
    private final Level logLevel;

    public Logger(String name, Level logLevel) {
        this.name = name;
        format = new SimpleDateFormat("yyyy-MM-dd");
        this.logLevel = logLevel;
    }

    public Logger(String name) {
        this(name, Level.INFO);
    }

    private synchronized  void log(Level level, String msg){
        if (level.lessThan(logLevel))
            return;

        var date = Calendar.getInstance().getTime();

        System.out.printf(
                "[%s][%5s][%5s] %s%n",
                format.format(date),
                name,
                level,
                msg
        );
    }

    public void debug(String msg){
        log(Level.DEBUG, msg);
    }

    public void info(String msg){
        log(Level.INFO, msg);
    }

    public void warning(String msg){
        log(Level.WARNING, msg);
    }

    public void error(String msg){
        log(Level.ERROR, msg);
    }

    public void fatal(String msg){
        log(Level.FATAL, msg);
    }

    public void progress(double percent1, double percent2){
        System.out.printf(
                "\r mapping: %.2f%%\treducing: %.2f%%",
                percent1*100, percent2*100
        );
    }

}
