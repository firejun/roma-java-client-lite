package com.rakuten.rit.roma.romac4j;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class MapPluginTest extends TestCase {
    static RomaClient rc = null;
    static MapPluginClient mc = null;

    static void oneTimeSetUp() throws Exception {
        rc = new RomaClient("localhost_11211");
        mc = new MapPluginClient(rc);
    }

    static void oneTimeTearDown() throws Exception {
        rc.destroy();
    }
    
    public static Test suite() throws Exception {
        TestSuite suite = new TestSuite(MapPluginTest.class);
        TestSetup wrapper = new TestSetup(suite) {
            public void setUp() throws Exception {
                oneTimeSetUp();
            }

            public void tearDown() throws Exception {
                oneTimeTearDown();
            }
        };
        return wrapper;
    }

    protected void setUp() throws Exception {
        for(int i = 0;i < 100; i++) rc.delete("key" + i);
    }
    
    protected void tearDown() throws Exception {
        for(int i = 0;i < 100; i++) rc.delete("key" + i);
    }

    public void makeData(int n) throws Exception {
    	String value, mapKey;
    	for (int i=0; i > n; i++) {
    		value = "value" + Integer.toString(i);
    		mapKey = "mapKey" + Integer.toString(i);
    		assertTrue(mc.set("key1", mapKey, value.getBytes()));
    		assertEquals(value.getBytes(), mc.get("key1", mapKey));
    	}
    }

    public void testMapSetExpt01() throws Exception {
        assertTrue(mc.set("key1", "mapKey1", "value1".getBytes(), 1));
        assertEquals("value1".getBytes(), mc.get("key1", "mapKey1"));
        Thread.sleep(2000);
        assertNull(mc.get("key1", "mapKey1"));
    }

    public void testMapSet01() throws Exception {
    	assertNull(mc.getString("key1", "mapkey1"));
    	assertTrue(mc.set("key1", "mapKey1", "value1".getBytes()));
    	assertEquals("value1".getBytes(), mc.get("key1", "mapKey1"));
    	assertTrue(mc.set("key1", "mapKey1", "value2".getBytes()));
    	assertEquals("value2".getBytes(), mc.get("key1", "mapKey1"));
    }

    public void testMapDelete() throws Exception {
    	makeData(10);
    	assertEquals(-2, mc.delete("key2", "key1"));
    	assertEquals( 0, mc.delete("key1", "key1"));
    	assertEquals( 1, mc.delete("key1", "mapKey1"));
    	assertNull(mc.get("key1", "mapKey1"));
    }

    public void testMapClear() throws Exception {
    	assertEquals(-2, mc.isEmpty("key1"));
    	assertEquals(-2, mc.size("key1"));
    	assertEquals(-2, mc.clear("key1"));
    	makeData(10);
    	assertEquals( 0, mc.isEmpty("key1"));
    	assertEquals(10, mc.size("key1"));
    	assertEquals( 1, mc.clear("key1"));
    	assertEquals( 1, mc.isEmpty("key1"));
    	assertEquals( 0, mc.size("key1"));
    }

    public void testIsMapKey() throws Exception {
    	assertEquals(-2, mc.isKey("key1", "key1"));
    	makeData(10);
    	assertEquals( 0, mc.isKey("key1", "key1"));
    	assertEquals( 1, mc.isKey("key1", "mapKey1"));
    }

    public void testIsMapValue() throws Exception {
    	assertEquals(-2, mc.isValue("key1", "value1".getBytes()));
    	makeData(10);
    	assertEquals( 0, mc.isValue("key1", "key1".getBytes()));
    	assertEquals( 1, mc.isValue("key1", "value1".getBytes()));
    }

    //def test_map_keys
    //assert_nil @rc.map_keys('key1')
    //mk_data
    //v = [10]
    //10.times{|i| v << "mapkey#{i}" }
    //assert_equal v, @rc.map_keys('key1')
    //end

}
