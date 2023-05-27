## 云计算课程实验
### 实验一 hadoop-new
1.	理解Hadoop体系结构。
2.	熟练掌握Hadoop平台的部署方法与配置。

### 实验二 MapReduce
1.	理解HDFS在Hadoop体系结构中的角色。
2.	熟练使用HDFS操作常用的Shell命令。
3.	熟悉HDFS操作常用的Java API。

### 实验三 HDFS分布式文件系统
1.	通过实验掌握基本的MapReduce编程方法。
2.	掌握用MapReduce解决一些常见的数据处理问题，包括数据去重、数据排序和数据挖掘等。
3.	通过操作MapReduce的实验，模仿实验内容，深入理解MapReduce的过程，和shuffle的具体意义。

### 实验四 Hbase数据库
1.	理解HBase在Hadoop体系结构中的角色。
2.	熟练使用HBase操作常用的Shell命令。

## 常用命令

### 启动命令

```
cd /usr/local/hadoop
```

```
rm -fr tmp/*
rm -fr hdfs/name/*
rm -fr hdfs/data/*
```

```
hadoop namenode -format
```

```
start-all.sh
jps
```

```
bin/hdfs dfsadmin -safemode get
bin/hdfs dfsadmin -safemode leave
```

## 实验二

```java
实验二代码：
package com.hdfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;
public class HdfsApi {
    /**
    *判断路径是否存在
    */
    public static boolean test(Configuration conf,String path) throws IOException{
        FileSystem fs = FileSystem.get(conf);
        return fs.exists(new Path(path));
    }
    /**
    *复制文件到指定路径
    *若路径已存在，则进行覆盖
    */
    public static void copyFromLocalFile(Configuration conf,String localFilePath,String remoteFilePath) throws IOException{
        FileSystem fs = FileSystem.get(conf);
        Path localPath = new Path(localFilePath);
        Path remotePath = new Path(remoteFilePath);
        /*fs.copyFromLocalFile第一个参数表示是否删除源文件，第二个参数表示是否覆盖*/
        fs.copyFromLocalFile(false,true,localPath,remotePath);
        /*床一个文件读入流*/
        FileInputStream in = new FileInputStream(localFilePath);
        /*创建一个文件输出流，输出的内容将追加到文件末尾*/
        FSDataOutputStream out = fs.append(remotePath);
        /*读写文件内容*/
        byte[] data = new byte[1024];
        int read = -1;
        while((read = in.read(data))>0){
            out.write(data,0,read);
        }
        out.close();
        in.close();
        fs.close();
    }
	/**
	*主函数
	*/
	public static void main(String[] args){
	    Configuration conf = new Configuration();
	    conf.set("fs.default.name","hdfs://localhost:9000");
	    String localFilePath = "/usr/local/hadoop/text.txt";//本地路径
	    String remoteFilePath = "/usr/hadoop/text.txt";//HDFS路径
	    String choice = "append";//若文件存在则追加到文件末尾
	    //String choice = "overwrite";若文件存在则覆盖
	    try{
	        /*判断文件是否存在*/
	        boolean fileExists = false;
	        if(HdfsApi.test(conf,remoteFilePath)){
	            fileExists = true;
	            System.out.println(remoteFilePath + "已存在.");
	        }else{
	            System.out.println(remoteFilePath + "不存在.");
	        }
	        /*进行处理*/
	        if(!fileExists){
	           //文件不存在，则上传
				HdfsApi.copyFromLocalFile(conf,localFilePath,remoteFilePath);
	            System.out.println(localFilePath + "已上传至" + remoteFilePath);
	        }else if(choice.equals("overwrite")){
	            //选择覆盖
	            HdfsApi.copyFromLocalFile(conf,localFilePath,remoteFilePath);  
	            System.out.println(localFilePath + "已覆盖" + remoteFilePath);
	        }else if(choice.equals("append")){
	            //选择追加
	            HdfsApi.copyFromLocalFile(conf,localFilePath,remoteFilePath);  
	            System.out.println(localFilePath + "已追加至" + remoteFilePath);
	        } 
	    }catch(Exception e){
	        e.printStackTrace();
	    }
	}
}
```

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;
import java.net.URI;
public class HdfsApi {
	/**
	 *判断路径是否存在
	 */
	public static boolean test(Configuration conf,String path)throws IOException{
		FileSystem fs = FileSystem.get(conf);
		return fs.exists(new Path(path));
	}
	/**
	 *复制文件到指定路径
	 *若路径已存在，则进行覆盖
	 */
	public static void copyFromLocalFile(Configuration conf,String localFilePath, String remoteFilePath)throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path localPath = new Path(localFilePath);
		Path remotePath = new Path(remoteFilePath);
		/*fs.copyFromLocalFile第一个参数表示是否删除源文件，第二个参数表示是否覆盖*/
		fs.copyFromLocalFile(false,true,localPath,remotePath);
		fs.close();
	}
	public static void appendToFile(Configuration conf ,String localFilePath, String remoteFilePath)throws IOException{
		FileSystem fs = FileSystem.get(conf);
		Path remotePath = new Path(remoteFilePath);
		/*创建一个文件读入流*/
		FileInputStream in = new FileInputStream(localFilePath);
		/*创建一个文件输出流，输出的内容将追加到文件末尾*/
		FSDataOutputStream out = fs.append(remotePath);
		/*读写文件内容*/
		byte[] data = new byte[1024];
		int read = -1;
		while((read = in.read(data))>0) {
			out.write(data,0,read);
		}
		out.close();
		in.close();
		fs.close();
	}
	/*
	 * 若文件存在，则进行删除
	 */
	public static void deleteFile(Configuration conf,String remoteFilePath) throws Exception {
        
		FileSystem fs = FileSystem.get(conf);
        // 指定要删除的文件路径
        Path path = new Path(remoteFilePath);
        // 删除文件
        boolean deleted = fs.delete(path, true);
        if (deleted) {
        	System.out.println(remoteFilePath + "删除成功！");
        } else {
        	System.out.println(remoteFilePath + "删除失败！");
        }
        // 关闭 FileSystem 对象
        fs.close();
    }
	/**
	 *主函数
	 */
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER");
		conf.set("dfs.client.block.write.repalce-datanode-on-failure.enable", "true");
		String localFilePath = "/usr/local/hadoop/local.txt";//本地路径
		String remoteFilePath = "/usr/local/hadoop/text.txt";//HDFS路径
		//String choice = "append";//若文件存在则追加到文件末尾
	    //String choice = "overwrite";//若文件存在则覆盖
	    String choice = "delete";//若文件存在则删除
		try {
			 /*判断文件是否存在*/
			boolean fileExists = false;
			if(HdfsApi.test(conf, remoteFilePath)) {
				fileExists = true;
				System.out.println(remoteFilePath + "已存在");
			}else {
				System.out.println(remoteFilePath + "不存在");
			}
			/*进行处理*/
			if(!fileExists) {
				//文件不存在，则上传
				HdfsApi.copyFromLocalFile(conf, localFilePath, remoteFilePath);
				System.out.println(localFilePath + "已上传至" + remoteFilePath);
			}else if(choice.equals("overwrite")) {
				//选择覆盖
				HdfsApi.copyFromLocalFile(conf, localFilePath, remoteFilePath);
				System.out.println(localFilePath + "已覆盖" + remoteFilePath);
			}else if(choice.equals("append")) {
				//选择追加
				HdfsApi.appendToFile(conf, localFilePath, remoteFilePath);
				System.out.println(localFilePath + "已追加至" + remoteFilePath);
			}else if(choice.equals("delete")){
				//选择删除
				HdfsApi.deleteFile(conf,remoteFilePath);
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}
```



## 实验三



在编译代码之前需要执行以下命令（启动hadoop服务之后）：

```shell
cd /usr/local/hadoop

hadoop fs -mkdir -p  /user/hadoop/input

hadoop fs -put f*.txt /user/hadoop/input
```

```java
实验三代码：
package com.Merge;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Created by Xie kang on 2023/4/14.
 */
public class Merge {
	/**
	 *对A、B两个文件进行合并，并剔除其中重复的内容，得到一个新的输出文件C
	 */
    //重载Map函数，直接将输入中的value复制到输出数据的key上
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private static Text text = new Text();

        public void map(Object key, Text value, Context content) throws IOException, InterruptedException {

            text = value;
            content.write(text, new Text(""));
        }
    }
	//重载Reduce函数，直接将输入中的key复制到输出数据的key上
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }


    public static void main(String[] args) throws Exception {
    	
    	Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        String[] otherArgs = new String[]{"input","output"};//设置输入参数
        if (otherArgs.length != 2) {
            System.err.println("Usage:Merge and duplicate removal <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf,"Merge and duplicate removal");
        job.setJarByClass(Merge.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
```

### 拓展实验

在编译代码之前需要执行以下命令（启动hadoop服务之后）：

```shell
cd /usr/local/hadoop
hadoop fs -mkdir -p  /user/hadoop/wordcount_input
hadoop fs -put wordcount.txt /user/hadoop/wordcount_input
```

拓展实验代码：

```java
WordCount代码
package com.WordCount;

import java.io.IOException;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by Xie kang on 2023/5/5.
 */
public class WordCount {
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      /**
         * Mapper中的map方法：
         * void map(K1 key, V1 value, Context context)
         * 映射一个单个的输入k/v对到一个中间的k/v对
         * 输出对不需要和输入对是相同的类型，输入对可以映射到0个或多个输出对。
         * Context：收集Mapper输出的<k,v>对。
         * Context的write(k, v)方法:增加一个(k,v)对到context
         * 程序员主要编写Map和Reduce函数.这个Map函数使用StringTokenizer函数对字符串进行分隔,通过write方法把单词存入word中
     * write方法存入(单词,1)这样的二元组到context中
     */ 
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
      /**
         * Reducer类中的reduce方法：
      * void reduce(Text key, Iterable<IntWritable> values, Context context)
         * 中k/v来自于map函数中的context,可能经过了进一步处理(combiner),同样通过context输出           
         */
    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
    
  public static void main(String[] args) throws Exception {
      /**
      * Configuration：map/reduce的j配置类，向hadoop框架描述map-reduce执行的工作
 	*/
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://localhost:9000");
    String[] otherArgs = new String[]{"wordcount_input","wordcount_output"};//设置输入参数
    if (otherArgs.length != 2) {
        System.err.println("Usage:Merge and duplicate removal <in> <out>");
        System.exit(2);
    }
    Job job = new Job(conf, "word count");//设置一个用于定义的job名称
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);//为job设置Mapper类
    job.setCombinerClass(IntSumReducer.class);//为job设置Combiner类
    job.setReducerClass(IntSumReducer.class);//为job设置Reducer类
    job.setOutputKeyClass(Text.class);//为job的输出数据设置Key类
    job.setOutputValueClass(IntWritable.class);//为job输出设置value类
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//为job设置输入路径
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//为job设置输出路径
    System.exit(job.waitForCompletion(true) ? 0 : 1);//运行job
  }
}
```

