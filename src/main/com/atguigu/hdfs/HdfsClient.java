package atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;


public class HdfsClient {
    @Test
    public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {

        // 1.用于配置HDFS相关的参数
        Configuration configuration = new Configuration();
        // 配置NameNode地址
        // configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
        //2.创建一个用于操作HDFS集群的 FileSystem对象
        // FileSystem fs = FileSystem.get(configuration);
        // 配置在集群上运行
        //configuration.set("fs.defaultFS","hdfs://192.168.1.102:9000");
        //FileSystem fs = FileSystem.get(configuration);
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.102:9000"),configuration,"atguigu");
        // 2 创建目录
        fs.mkdirs(new Path("/05134/gaoxinban"));
        // 3 关闭资源
        fs.close();
    }


    /**
     * 文件上传
     */
    @Test
    public void  testCopyFromLocalFile()throws IOException, InterruptedException, URISyntaxException{
        //1.HDFS参数配置
        Configuration configuration  =new Configuration();
        //2.创建操作HDFS文件系统对象new URI("hdfs://192.168.1.102:9000")
        FileSystem fileSystem =FileSystem.get(new URI("hdfs://192.168.1.102:9000"),configuration,"atguigu");
        //3.copy
        fileSystem.copyFromLocalFile(new Path("D:/test.txt"),new Path("/0513/gaoxinban"));
        //4.关闭资源
        fileSystem.close();
    }
    /**
     * 修改文件名
     * rename
     */

    /**
     * 显示文件信息
     */
    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException{
        //1.HDFS参数配置
        Configuration configuration  =new Configuration();
        //2.创建操作HDFS文件系统对象new URI("hdfs://192.168.1.102:9000")
        FileSystem fileSystem =FileSystem.get(new URI("hdfs://192.168.1.102:9000"),configuration,"atguigu");
        //3.显示详情
        RemoteIterator<LocatedFileStatus> listFiles= fileSystem.listFiles(new Path("/"),true);
        //迭代
        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus=listFiles.next();
            System.out.println("访问时间："+ new Date( fileStatus.getAccessTime()));
            System.out.println("块大小："+fileStatus.getAccessTime());
        }
        //4.释放资源
        fileSystem.close();
    }
}
