package process;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.deploy.SparkSubmit;
import util.HDFSUtil;

import java.io.IOException;

/**
 * Created by zhaokangpan on 16/9/15.
 */
public class SparkSubmitProcess {

    //定义本地根目录
    static String src_dir = "/Users/zhaokangpan/Documents/";

    //定义hdfs上的根目录
    static String root_dir = "hdfs://127.0.0.1:9000/user/zhaokangpan/";

    //实例化工具类
    HDFSUtil hdfsUtil = new HDFSUtil();

    Configuration conf = new Configuration();

    public void trainLda(String file_name){

        //清除集群上同一目录下的同名文件
        hdfsUtil.checkAndDel(root_dir + file_name, conf);

        //清除上次的训练结果
        hdfsUtil.checkAndDel(root_dir + "weibo", conf);

        //上传文件至hdfs
        hdfsUtil.put2HSFS( src_dir + file_name ,  root_dir ,  conf);

        //配置spark训练参数
        String[] args = new String[]{
                "--class","spark.mllib.SparkGibbsLDAWeibo",
                "lib/SparkGibbsLDAWeibo.jar",
                root_dir,    //args(0) hdfs上的根目录
                root_dir + file_name,  //args(1) 训练文件的路径
                "10",//args(2) topic数量
                "100",//args(3) 最大训练次数
                "110",//args(4) 为了释放内存迭代过程中的每110次重启spark上下文对象,这里用小样本测试不需要重启.
                "spark://127.0.0.1:7077"//args(5) master进程地址
        };

        //提交作业
        SparkSubmit.main(args);
    }

    public void predictLda(String text) throws IOException{

        //清除集群上同一目录下的同名文件
        hdfsUtil.checkAndDel(root_dir + "weibo/out/theta", conf);

        //配置spark训练参数
        String[] args = new String[]{
                "--class","spark.mllib.SparkGibbsLDAPredict",
                "lib/SparkGibbsLDAWeibo.jar",
                "spark://127.0.0.1:7077",//args(0) masterip
                root_dir,//args(1) 根目录位置
                text,//args(2) 待推断的文本
                "@#@",//args(3) 文本分割符,可自定义
        };

        //提交作业
        SparkSubmit.main(args);

        //读取预测结果
        String result = hdfsUtil.readFromHDFS(root_dir + "weibo/out/theta/part-00000", conf);
        //检测结果
        System.out.println(result);
    }

    public void relevanceCalcualtion(String file_name, String num_word) throws IOException{

        //清除上次的训练结果
        hdfsUtil.checkAndDel(root_dir + "weibo/out/relevanceCalculation", conf);

        //配置spark训练参数
        String[] args = new String[]{
                "--class","spark.mllib.UserCombine",
                "lib/SparkGibbsLDAWeibo.jar",
                file_name,//args(0) 与训练集相应的文本集
                num_word, //args(1) queryset中词的个数
                root_dir, //args(2) location on hdfs
                "spark://localhost:7077",//args(3) masterip
        };

        //提交作业
        SparkSubmit.main(args);

        //读取计算结果(一共ktopic + 2项) 第一项是用户ID,程序里面写死了,展示的时候可以不要这个字段, 中间是ktopic个概率分布, 最后一项是所有分布值加合
        String result = hdfsUtil.readFromHDFS(root_dir + "weibo/out/relevanceCalculation/part-00000", conf);
    }

    public static void main(String[] args)throws IOException{
        SparkSubmitProcess tt = new SparkSubmitProcess();
        String file_name = "weiboLdaTest.txt";
        String num_word = "1000";
        //tt.trainLda(file_name);

        String text = "嘻嘻 哈哈 天天向上@#@抄 手 北京 美食 海淀 吃 美食 攻 编 盘点 海淀 吃 美食 冰山 一角 美食 等待 去 发现";
        //tt.predictLda(text);

        tt.relevanceCalcualtion(file_name, num_word);
    }
}
