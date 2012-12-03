import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.RomaClient;

public class Start {
	protected static Logger log = Logger.getLogger(Start.class.getName());
	public static void main(String argv[]) throws Exception {
		RomaClient rc = new RomaClient();
		byte[] b = null;
		long time0 = System.currentTimeMillis();
		try {
			for (int i=0; i < 500000; i++) {
				b = rc.get("foo");
				log.debug(new String(b));
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		time0 = System.currentTimeMillis() - time0;
		log.debug("Elap Time: " + time0/1000.0);
    }
}
