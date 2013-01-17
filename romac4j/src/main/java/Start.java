import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.Cas;
import com.rakuten.rit.roma.romac4j.RomaClient;
import com.rakuten.rit.roma.romac4j.ValueReceiver;
import com.rakuten.rit.roma.romac4j.utils.PropertiesUtils;

public class Start {
    protected static Logger log = Logger.getLogger(Start.class.getName());
    
    public static void main(String argv[]) throws Exception {
        RomaClient rc = new RomaClient(PropertiesUtils.getRomaClientProperties());
        byte[] b = null;
        rc.set("foo", "test1".getBytes(), 0);

        boolean res = rc.cas("foo", 0, new Cas(null) {
            public byte[] cas(ValueReceiver rcv) {
                return "test2".getBytes();
            }
        });
        log.debug("res: " + res);

        long time0 = System.currentTimeMillis();
        for (int i = 0; i < 5000; i++) {
            try {
                b = rc.get("foo");
                log.debug(new String(b));
            } catch (Exception e) {
                log.debug("Main Error: " + e.getMessage());
            }
            Thread.sleep(1000);
        }
        time0 = System.currentTimeMillis() - time0;
        log.debug("Elap Time: " + time0 / 1000.0);

        rc.destroy();
    }
}
