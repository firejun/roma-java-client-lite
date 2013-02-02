package com.rakuten.rit.roma.romac4j;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class AListPluginTest extends TestCase {
    static RomaClient rc = null;
    static AListPluginClient ac = null;

    static void oneTimeSetUp() throws Exception {
        rc = new RomaClient("localhost_11211");
        ac = new AListPluginClient(rc);
    }

    static void oneTimeTearDown() throws Exception {
        rc.destroy();
    }
    
    public static Test suite() throws Exception {
        TestSuite suite = new TestSuite(AListPluginTest.class);
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

    public void testPushGets01() throws Exception {
        String[] res;
        assertEquals(0, ac.getsString("key1").length); // the key dose't exist
        
        assertTrue(ac.push("key1", "0")); // first data
        res = ac.getsString("key1");
        assertEquals(1, res.length);
        assertEquals("0", res[0]);
        
        for(int i = 0;i < 9; i++){
            assertTrue(ac.push("key1", "" + (i + 1))); // add data
        }
        res = ac.getsString("key1");
        assertEquals(10, res.length);
        for(int i = 0;i < 10; i++){
            assertEquals("" + i, res[i]);
        }
    }
    
    public void testAt01() throws Exception {
        for(int i = 0;i < 10; i++){
            assertNull(ac.atString("key1", i)); // the key dose't exist
        }
        for(int i = 0;i < 10; i++){
            assertTrue(ac.push("key1", "" + i)); // add data
        }
        for(int i = 0;i < 10; i++){
            assertEquals("" + i, ac.atString("key1", i));
        }
        assertNull(ac.atString("key1", 11)); // out of range
        assertEquals("9" ,ac.atString("key1", -1)); // returns last
        assertEquals("8" ,ac.atString("key1", -2)); // returns second to last
        assertEquals("7" ,ac.atString("key1", -3)); // returns third to last
    }
    
    public void testClearEmpty01() throws Exception {
        assertEquals(2, ac.isEmpty("key1")); // 2:the key dose't exist
        for(int i = 0;i < 10; i++){
            assertTrue(ac.push("key1", "" + i)); // add data
        }
        assertEquals(0, ac.isEmpty("key1")); // 0:not empty
        assertTrue(ac.clear("key1"));
        assertEquals(1, ac.isEmpty("key1")); // 1:empty
    }
    
    public void testClearEmpty02() throws Exception {
        assertEquals(2, ac.isEmpty("key1")); // 2:the key dose't exist
        assertFalse(ac.clear("key1")); // returns false when the key dose't exist
    }
    
    public void testJoin01() throws Exception {
        assertNull(ac.join("key1")); // the key dose't exist
        assertTrue(ac.push("key1", "0")); // add data
        assertEquals("0", ac.join("key1"));
        for(int i = 0;i < 9; i++){
            assertTrue(ac.push("key1", "" + (i + 1))); // add data
        }
        assertEquals("0,1,2,3,4,5,6,7,8,9", ac.join("key1"));
        assertEquals("0|1|2|3|4|5|6|7|8|9", ac.join("key1", "|"));
    }
 
    public void testToS01() throws Exception {
        assertNull(ac.toS("key1")); // the key dose't exist
        assertTrue(ac.push("key1", "0")); // add data
        assertEquals("[\"0\"]", ac.toS("key1"));
        for(int i = 0;i < 9; i++){
            assertTrue(ac.push("key1", "" + (i + 1))); // add data
        }
        assertEquals("[\"0\", \"1\", \"2\", \"3\", \"4\", \"5\", \"6\", \"7\", \"8\", \"9\"]", ac.toS("key1"));
    }

    public void testDelete01() throws Exception {
        assertFalse(ac.delete("key1", "1")); // the key dose't exist
        for(int i = 0;i < 10; i++){
            assertTrue(ac.push("key1", "" + i)); // add data
        }
        assertFalse(ac.delete("key1", "10")); // the value dose't exist
        assertEquals("0,1,2,3,4,5,6,7,8,9", ac.join("key1"));

        assertTrue(ac.delete("key1", "9"));
        assertEquals("0,1,2,3,4,5,6,7,8", ac.join("key1"));

        assertTrue(ac.delete("key1", "5"));
        assertEquals("0,1,2,3,4,6,7,8", ac.join("key1"));
    }
    
    public void testDeleteAt01() throws Exception {
        assertFalse(ac.deleteAt("key1", 1)); // the key dose't exist
        for(int i = 0;i < 10; i++){
            assertTrue(ac.push("key1", "" + i)); // add data
        }
        assertFalse(ac.deleteAt("key1", 10)); // out of range
        
        assertTrue(ac.deleteAt("key1", 3));
        assertEquals("0,1,2,4,5,6,7,8,9", ac.join("key1"));
        
        assertTrue(ac.deleteAt("key1", -1)); // delete for second to last
        assertEquals("0,1,2,4,5,6,7,8", ac.join("key1"));
    
        assertTrue(ac.deleteAt("key1", -2)); // delete for third to last
        assertEquals("0,1,2,4,5,6,8", ac.join("key1"));
    }

    public void testFirstLast01() throws Exception {
        assertNull(ac.firstString("key1")); // the key dose't exist
        assertNull(ac.lastString("key1")); // the key dose't exist

        assertTrue(ac.push("key1", "0")); // add data
        assertEquals("0", ac.firstString("key1"));
        assertEquals("0", ac.lastString("key1"));

        assertTrue(ac.push("key1", "1")); // add data
        assertEquals("0", ac.firstString("key1"));
        assertEquals("1", ac.lastString("key1"));
        
        assertTrue(ac.push("key1", "2")); // add data
        assertEquals("0", ac.firstString("key1"));
        assertEquals("2", ac.lastString("key1"));
    }
}
