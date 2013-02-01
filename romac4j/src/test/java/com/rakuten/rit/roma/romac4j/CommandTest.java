package com.rakuten.rit.roma.romac4j;

import java.util.Map;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class CommandTest extends TestCase {
    static RomaClient rc = null;

    static void oneTimeSetUp() throws Exception {
        rc = new RomaClient("localhost_11211");
    }

    static void oneTimeTearDown() throws Exception {
        rc.destroy();
    }
    
    public static Test suite() throws Exception {
        TestSuite suite = new TestSuite(CommandTest.class);
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

    public void testSetGet01() throws Exception {
        String value = null;
        
        for(int i = 0;i < 10;i++){
            assertTrue(rc.set("key" + i, ("val" + i).getBytes(), 0));
            value = new String(rc.get("key" + i));
            assertEquals("val" + i, value);
        }

        assertTrue(rc.set("key1", "".getBytes(), 0));
        value = new String(rc.get("key1"));
        assertEquals("", value);        
    }

    public void testSetGet02() throws Exception {
        for(int i = 0;i < 10;i++){
            assertTrue(rc.set("key" + i, "val" + i, 0));
            assertEquals("val" + i, rc.getString("key" + i));
        }

        assertTrue(rc.set("key1", "", 0));
        assertEquals("", rc.getString("key1"));        
    }

    public void testSetGets01() throws Exception {
        String[] keys = new String[10];
        for(int i = 0;i < 10; i++){
            keys[i] = "key" + i; 
            assertEquals(null, rc.getString(keys[i]));
            assertTrue(rc.set(keys[i], "val" + i, 0));
        }
        Map<String, String> m = rc.getsString(keys);
        assertEquals(10, m.size());
        for(int i = 0;i < 10; i++){
            assertEquals("val" + i, m.get(keys[i]));
        }
        
        m = rc.getsString(new String[]{"key10", "key11", "key12"});
        assertEquals(0, m.size());
        m = rc.getsString(new String[]{"key0", "key11", "key12"});
        assertEquals(1, m.size());
        m = rc.getsString(new String[]{"key0", "key1", "key12"});
        assertEquals(2, m.size());
        m = rc.getsString(new String[]{"key0", "key1", "key2"});
        assertEquals(3, m.size());
    }
    
    public void testAppend01() throws Exception {
        assertTrue(rc.set("key1", "01", 0));
        assertTrue(rc.append("key1", "02", 0));
        assertTrue(rc.append("key1", "03", 0));
        assertTrue(rc.append("key1", "04", 0));
        assertEquals("01020304", rc.getString("key1"));
    }
    
    public void testCas01() throws Exception {
        assertTrue(rc.set("key1", "1", 0));
        assertEquals("1", rc.getString("key1"));
        boolean res = rc.cas("key1", 0, new Cas(null) {
            public byte[] cas(ValueReceiver rcv) {
                int n = Integer.parseInt(rcv.getValueString());
                String ret = "" + (n + 1);
                return ret.getBytes();
            }
        });
        assertTrue(res);
        assertEquals("2", rc.getString("key1"));
    }

    public void testCas02() throws Exception {
        assertEquals(null, rc.getString("key1"));   // not exist key
        boolean res = rc.cas("key1", 0, new Cas(null) {
            public byte[] cas(ValueReceiver rcv) {
                int n = Integer.parseInt(rcv.getValueString());
                String ret = "" + (n + 1);
                return ret.getBytes();
            }
        });
        assertFalse(res);
    }
}