package map_reduce;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

/** @noinspection unchecked*/
public class MapViewer {

    private static final String filename =
            "word count";

    public static void main(String[] args) throws IOException, ClassNotFoundException {



        var fis = new FileInputStream(filename + ".reduce");

        var ois = new ObjectInputStream(fis);

        if (!(ois.readObject() instanceof Map map))
            return;

        map.forEach((k, v)->
            System.out.printf("%s: %s\n", k, v)
        );
    }
}
