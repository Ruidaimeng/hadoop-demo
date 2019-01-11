package com.imooc.hadoop.demoApp;/*
 * Description
 *  HDFS JAVA API 操作
 *@author Ruimeng
 *@Date 2019/1/5 14:51
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

public class HDFSApp {

    //注意这里的协议是 ，hdfs 不是http ==!!!!!!
    public static final String HDFS_PATH = "hdfs://10.20.149.176:8020";
    FileSystem fileSystem = null ;
    Configuration configuration =null;

/*
*  创建文件夹
* */
    @Test
    public void mkDir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test3"));

    }

    /*
     *  创建文件
     * */
    @Test
    public void create() throws Exception{
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        //将内容写到文件中去
        output.write("hello hadoop".getBytes());
        output.flush();
        output.close();
    }
    /*
     *  查看文件内容
     * */
    @Test
    public void catFile() throws Exception{
        FSDataInputStream input = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        //将内容写到文件中去
        IOUtils.copyBytes(input,System.out,1024);
        input.close();
    }

    /*
     *  重命名操作
     * */
    @Test
    public void renameFile() throws Exception{
       Path oldPath= new Path("/hdfsapi/test/a.txt");
       Path newPath= new Path("/hdfsapi/test/b.txt");
        fileSystem.rename(oldPath ,newPath);
    }

    /*
     *  上传文件到HDFS
     * */
    @Test
    public void uploadFile() throws Exception{
        Path localPath= new Path("E:/123.txt");
        Path hdfsPath= new Path("/hdfsapi/test/234.txt");
        fileSystem.copyFromLocalFile(localPath,hdfsPath);
    }

    /*
     *  上传文件到HDFS==带进度条
     * */
    @Test
    public void uploadFilewithProces() throws Exception{
        Path localPath= new Path("E:/123.txt");
        Path hdfsPath= new Path("/hdfsapi/test/234.txt");
        fileSystem.copyFromLocalFile(localPath,hdfsPath);
        //输入流
        InputStream in = new BufferedInputStream(new FileInputStream(new File("E:/babaLinux.ova")));
        //输出流
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/a.ova"), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in,output,4096);
    }


    /*
     *  下载文件到本地
     *
     * */
    @Test
    public void downloadFile() throws Exception{
        Path localPath= new Path("E:/12.txt");
        Path hdfsPath= new Path("/hdfsapi/test/b.txt");
        fileSystem.copyToLocalFile(hdfsPath ,localPath);
    }

    /*
     *  查看 hdfs，上的文件目录
     *
     * */
    @Test
    public void listFiles() throws Exception{

      FileStatus[] fileStatuss = fileSystem.listStatus(new Path("hdfs/test"));
        for (FileStatus fileStatus:fileStatuss) {
         String isDir =   fileStatus.isDirectory()?"Dir":"File";
           short replication = fileStatus.getReplication();  //副本个数
            fileStatus.getLen(); //文件大小
            fileStatus.getPath();//文件路径

        }

       // fileSystem.copyToLocalFile();
    }

    /*
     *  删除文件夹
     * */
    @Test
    public void delDir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test3"));

    }

    /*
     *  删除文件
     *
     *  '
     * */
    @Test
    public void delFile() throws Exception{
        //true指定是否递归删除！
        fileSystem.delete(new Path("/hdfsapi/test2"),true);

    }




    @Before
    public void setUp() throws Exception{
        System.out.println("HDFS======>setUp");
        configuration = new Configuration();
        fileSystem =FileSystem.get(new URI(HDFS_PATH ),configuration,"root");

    }

    @After
    public void tearDown() throws Exception{
        configuration =null;
        fileSystem = null ;
        System.out.println("HDFS======>tearDown");
    }
}
