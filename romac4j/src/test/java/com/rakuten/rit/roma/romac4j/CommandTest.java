package com.rakuten.rit.roma.romac4j;

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
        rc.delete("testSetGet01");
        rc.delete("testAppend01");
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

    public void testSetGet01() throws Exception {
        String value = null;
        assertTrue(rc.set("testSetGet01", "testSetGet01-1".getBytes(), 0));
        value = new String(rc.get("testSetGet01"));
        assertEquals("testSetGet01-1", value);
        assertTrue(rc.set("testSetGet01", "testSetGet01-2".getBytes(), 0));
        value = new String(rc.get("testSetGet01"));
        assertEquals("testSetGet01-2", value);
        assertTrue(rc.set("testSetGet01", "".getBytes(), 0));
        value = new String(rc.get("testSetGet01"));
        assertEquals("", value);        
    }

    public void testAppend01() throws Exception {
        assertTrue(rc.set("testAppend01", "01".getBytes(), 0));
        assertTrue(rc.append("testAppend01", "02".getBytes(), 0));
        assertTrue(rc.append("testAppend01", "03".getBytes(), 0));
        assertTrue(rc.append("testAppend01", "04".getBytes(), 0));
        String value = new String(rc.get("testAppend01"));
        assertEquals("01020304", value);
    }
}