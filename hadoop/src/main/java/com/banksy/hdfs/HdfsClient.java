package com.banksy.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * 1.创建文件夹
 * 2.文件上传
 * 3.文件下载
 * 4.文件删除
 * 5.文件更名及移动
 * 6.获取文件详细信息
 * 7.判断是文件夹还是文件
 */
public class HdfsClient {
    private FileSystem fs;

    @Before
    //  初始化配置
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 连接的集群nn地址
        URI uri = new URI("hdfs://172.16.123.11:8020");
        // TODO 创建一个配置文件
        Configuration configuration = new Configuration();
        //设置副本数量为2
        configuration.set("dfs.replication", "2");
        // 用户
        String user = "banksy";
        // 获取到客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    //  关闭资源
    public void close() throws IOException {
        // TODO 关闭资源
        fs.close();
    }

    //  1.创建文件夹
    @Test
    public void testMkdir() throws IOException {
        // 创建一个文件夹
        fs.mkdirs(new Path("/test/createFile1"));
    }

    // TODO 参数优先级，内置默认 < hdfs-site.xml < resources中的hdfs-size.xml < 代码中的设置
    //  2.文件上传
    @Test
    public void testPut() throws IOException {
        //参数解读：参数一：表示删除原数据；参数二：是否允许覆盖；参数三：原数据路径；参数四：目的地路径；
        fs.copyFromLocalFile(false, true,
                new Path("/Users/zevin_y/BoxFile/note.txt"),
                new Path("hdfs://hadoop111/test/createFile"));
    }

    //  3.文件下载
    @Test
    public void testGet() throws IOException {
        //参数解读：参数一：原文件是否删除；参数二：原文件路径HDFS；参数三：目标地址路径；参数四：是否开启校验；
        fs.copyToLocalFile(false,
                new Path("hdfs://hadoop111/test/createFile/note.txt"),
                new Path("/Users/zevin_y/BoxFile/123/"));
    }

    //  4.文件删除
    @Test
    public void testRm() throws IOException {
        //参数解读：参数一：要删除的路径；参数二：是否递归删除；
        fs.delete(new Path("hdfs://hadoop111/test/createFile/note.txt"), false);
    }

    //  5.文件更名及移动
    @Test
    public void testMv() throws IOException {
        //参数解读：参数一：原文件路径；参数二：目标文件路径；
        fs.rename(new Path("hdfs://hadoop111/test/createFile/note.txt"),
                new Path("hdfs://hadoop111/test/createFile/note1.txt"));
    }

    //  6.获取文件详细信息
    @Test
    public void fileDetail() throws IOException {
        //获取所有文件信息
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("hdfs://hadoop111/"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("==========" + fileStatus.getPath() + "==========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());
            //获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    //  7.判断是文件夹还是文件
    @Test
    public void testFile() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus status : listStatus) {
            if (status.isFile()){
                System.out.println("文件" + status.getPath().getName());
            }else {
                System.out.println("目录" + status.getPath().getName());
            }

        }
    }

}
