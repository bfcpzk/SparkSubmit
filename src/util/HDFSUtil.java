package util;

import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * Created by zhaokangpan on 16/9/14.
 */
public class HDFSUtil {

    /**
     * 文件上传
     * @param src
     * @param dst
     * @param conf
     * @return
     */
    public boolean put2HSFS(String src , String dst , Configuration conf){
        Path dstPath = new Path(dst) ;
        try{
            FileSystem hdfs = dstPath.getFileSystem(conf) ;
            hdfs.copyFromLocalFile(false, new Path(src), dstPath) ;
        }catch(IOException ie){
            ie.printStackTrace() ;
            return false ;
        }
        return true ;
    }

    /**
     * 文件下载
     * @param src
     * @param dst
     * @param conf
     * @return
     */
    public boolean getFromHDFS(String src , String dst , Configuration conf){
        Path dstPath = new Path(dst) ;
        try{
            FileSystem hdfs = dstPath.getFileSystem(conf) ;
            hdfs.copyToLocalFile(false, new Path(src), dstPath) ;
        }catch(IOException ie){
            ie.printStackTrace() ;
            return false ;
        }
        return true ;
    }

    /**
     * 文件检测并删除
     * @param path
     * @param conf
     * @return
     */
    public boolean checkAndDel(final String path , Configuration conf){
        Path dstPath = new Path(path) ;
        try{
            FileSystem hdfs = dstPath.getFileSystem(conf) ;
            if(hdfs.exists(dstPath)){
                hdfs.delete(dstPath, true) ;
            }else{
                return false ;
            }
        }catch(IOException ie ){
            ie.printStackTrace() ;
            return false ;
        }
        return true ;
    }

    public String readFromHDFS(String file, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        FSDataInputStream hdfsInStream = fs.open(new Path(file));
        byte[] ioBuffer = new byte[1];
        int readLen = hdfsInStream.read(ioBuffer);
        String result = "";
        while(-1 != readLen){
            result += new String(ioBuffer);
            readLen = hdfsInStream.read(ioBuffer);
        }
        hdfsInStream.close();
        fs.close();

        return result;
    }


    /**
     * @param args
     */
    public static void main(String[] args) {
        HDFSUtil hdfs = new HDFSUtil();
        String dst = "hdfs://127.0.0.1:9000/user/zhaokangpan" ;
        String src = "/Users/zhaokangpan/Documents/weiboLdaTest.txt" ;
        boolean status = false;
        Configuration conf = new Configuration();
        //status = hdfs.put2HSFS( src ,  dst ,  conf);
        System.out.println("status="+status);

        /*src = "hdfs://xcloud:9000/user/xcloud/out/loadtable.rb" ;
        dst = "/tmp/output" ;
        status = getFromHDFS( src ,  dst ,  conf) ;
        System.out.println("status="+status) ;*/

        dst = "hdfs://127.0.0.1:9000/user/zhaokangpan/weibo" ;
        status = hdfs.checkAndDel( dst ,  conf) ;
        System.out.println("status="+status) ;
    }

}

