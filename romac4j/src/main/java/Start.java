import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.RomaClient;

public class Start {
	protected static Logger log = Logger.getLogger(Start.class.getName());
	public static void main(String argv[]) throws Exception {
		RomaClient rc = new RomaClient();
		byte[] b = null;
		long time0 = System.currentTimeMillis();
		for (int i=0; i < 5000; i++) {
			try {
				b = rc.get("foo");
				log.debug(new String(b));
			} catch (Exception e) {
				log.error("Client Error.");
			}
			Thread.sleep(1000);
		}


		time0 = System.currentTimeMillis() - time0;
		log.debug("Elap Time: " + time0/1000.0);
		rc.destroy();
    }
}
