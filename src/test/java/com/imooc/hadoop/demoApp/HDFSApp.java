package com.imooc.hadoop.demoApp;/*
 * Description
 *  HDFS JAVA API 操作
 *@author Ruimeng
 *@Date 2019/1/5 14:51
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.net.URI;

public class HDFSApp {

    //注意这里的协议是 ，hdfs 不是http ==!!!!!!
    public static final String HDFS_PATH = "hdfs://140.143.242.234:8020";
    FileSystem fileSystem = null ;
    Configuration configuration =null;


    @Test
    public void mkDir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test"));

    }

    @Before
    public void setUp() throws Exception{
        System.out.println("HDFS======>setUp");
        configuration = new Configuration();
        fileSystem =FileSystem.get(new URI(HDFS_PATH ),configuration);

    }

    @After
    public void tearDown() throws Exception{
        configuration =null;
        fileSystem = null ;
        System.out.println("HDFS======>tearDown");
    }
}
