package com.wja.hadoopLearn.hdfs;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * HDFS 文件系统操作示例
 * 
 * @author wja
 */
public class HdfsDemo
{
    /**
     * 创建目录
     */
    public static void mkdir(String dir)
        throws IOException
    {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path f = new Path(dir);
        if (!fs.exists(f))
        {
            if (fs.mkdirs(f))
            {
                System.out.println("目录 " + dir + " 创建成功！");
            }
            else
            {
                System.out.println("目录 " + dir + " 创建失败！");
            }
        }
        else
        {
            System.out.println("目录 " + dir + " 已存在。");
        }
    }
    
    /**
     * 创建并写入文件
     */
    public static void createFile(String fn, String content)
        throws IOException
    {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path f = new Path(fn);
        FSDataOutputStream out = fs.create(f);
        // FSDataOutputStream out = fs.create(f, false);
        out.writeUTF(content);
        out.hflush();
        out.close();
        
        System.out.println("文件 " + fn + " 写入成功");
    }
    
    /**
     * 文件追加
     */
    public static void appendToFile(String fn, String content)
        throws IOException
    {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path f = new Path(fn);
        FSDataOutputStream out = fs.append(f);
        out.writeUTF(content);
        out.hflush();
        out.close();
        System.out.println("文件 " + fn + " 写入成功");
    }
    
    /**
     * 读文件
     */
    public static void readFile(String fn)
        throws IOException
    {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path f = new Path(fn);
        FSDataInputStream in = fs.open(f);
        boolean isend = false;
        while (!isend)
        {
            try
            {
                System.out.print(in.readUTF());
            }
            catch (EOFException e)
            {
                isend = true;
            }
        }
        in.close();
        System.out.println();
        System.out.println("文件 " + fn + " 读完成。");
    }
    
    /**
     * 清空文件
     */
    public static void truncateFile(String fn)
        throws IOException
    {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path f = new Path(fn);
        fs.truncate(f, 0);
        System.out.println("文件 " + fn + " 已被清空。");
    }
    
    /**
     * 判断文件、目录是否存在
     */
    public static void exists(String fn)
        throws IOException
    {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path f = new Path(fn);
        System.out.println("文件 " + fn + (fs.exists(f) ? " 存在。" : " 不存在。"));
    }
    
    public static void copyFromLocalFile(String source, String dest)
        throws IOException
    {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path s = new Path(source);
        Path d = new Path(dest);
        fs.copyFromLocalFile(s, d);
        System.out.println("从本地文件 " + source + " 拷贝到 " + dest + " 完成。");
    }
    
    /**
     * 删除文件、目录
     */
    public static void deleteFile(String fn)
        throws IOException
    {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path f = new Path(fn);
        if (fs.deleteOnExit(f))
        {
            System.out.println("文件 " + fn + " 已被删除。");
        }
        fs.close();
    }
    
    public static void main(String[] args)
        throws IOException
    {
        mkdir("/user/wja");
        String fn = "/user/wja/test.txt";
        createFile(fn, "aaaaaaaaaaaaaaa啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊");
        appendToFile(fn, "\naaaaaaaaaaaaaaa啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊");
        readFile(fn);
        truncateFile(fn);
        readFile(fn);
        // deleteFile(fn);
        // exists(fn);
        
        // deleteFile("/user/wja");
        // exists("/user/wja");
        
    }
    
}
