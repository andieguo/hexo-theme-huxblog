---
title: Hadoop之单表关联查询
date: 2014-08-01 11:02:53
description: "本案例主要通过输入文件中的child字段和parent字段进行单表关联查询，推导出哪些用户具有child与grandparent关系。"
tags: Hadoop
---

本案例主要通过输入文件中的child字段和parent字段进行单表关联查询，推导出哪些用户具有child与grandparent关系。

### 开发环境 ###
---

硬件环境：Centos 6.5 服务器4台（一台为Master节点，三台为Slave节点）  
软件环境：Java 1.7.0_45、hadoop-1.2.1

### 1、	输入数据分析 ###
---

输入文件数据示例：

```
    child parent
    Tom Jack 
    Jack Alice
    Jack Jesse
```

第1列表示child，第2列表示parent，我们需要根据child和parent的关系得出child和grantparent的关系。比如说Tom的parent是Jack，Jack的parent是Alice和Jesse，由此我们可以得出Tom的grantparent是{Alice，Jesse}。


### 2、	Map过程 ###
---

首先使用默认的TextInputFormat类对输入文件进行处理，得到文本中每行的偏移量及其内容。Map过程首先将输入分割成child和parent，然后正序输出一次作为右表，反序输出一次作为左表，需要注意的是在输出的value中必须加上左右表区别标志，其中左表标识符为1，右表标识符为2，如图所示。

![Map过程](map.png "Map过程")

Map端核心代码实现如下，详细源码请参考：SingletonTableJoin\src\com\zonesion\tablejoin\SingletonTableJoin.java。
```
    @Override
    protected void map(Object key, Text value, Context context)
    		throws IOException, InterruptedException {
    	String childName = new String();
    	String parentName = new String();
    	String relationType = new String();
    	String line = value.toString();
    	String[] values = line.split(" ");
    	if(values.length >= 2){
    		if(values[0].compareTo("child") != 0){
    			childName = values[0];
    			parentName = values[1];
    			relationType = "1";
    			context.write(new Text(parentName), new Text(relationType+" "+childName));//<"Lucy","1 Tom">
    			relationType = "2";
    			context.write(new Text(childName), new Text(relationType+" "+parentName));//<"Jone","2 Lucy">
    		}
    	}
    }
```

### 3、	Reduce过程 ###
---
Reduce过程首先对输入<key,values>即<"Lucy",["1 Tom","2 Mary","2 Ben"]>的values值进行遍历获取到单元信息（例如"1 Tom"），然后将单元信息中的用户ID（例如Tom）按照左表、右表标识符分别存入到grandChild集合和grandParent集合，最后对grandChild集合和grandParent集合进行笛卡尔积运算得到child与grandParent的关系，并进行输出，如图所示。

![Reduce过程](/images/20140801/single-table-join/reduce.png "Reduce过程")

Reduce端核心代码如下，详细源码请参考：SingletonTableJoin\src\com\zonesion\tablejoin\SingletonTableJoin.java。
```
    public static class JoinReducer extends Reducer<Text, Text, Text, Text>{
    
    	@Override
    	protected void reduce(Text key, Iterable<Text> values,Context context)
    			throws IOException, InterruptedException {
    		List<String> grandChild = new ArrayList<String>();//孙子
    		List<String> grandParent = new ArrayList<String>();//爷爷
    		Iterator<Text> it = values.iterator();//["1 Tom","2 Mary","2 Ben"]
    		while(it.hasNext()){
    			String[] record = it.next().toString().split(" ");//"1 Tom"---[1,Tom]
    			if(record.length == 0) continue;
    			if(record[0].equals("1")){//左表，取出child放入grandchild
    				grandChild.add(record[1]);
    			}else if(record[0].equals("2")){//右表，取出parent放入grandParent
    				grandParent.add(record[1]);
    			}
    		}
    		//grandchild 和 grandparent数组求笛卡尔积
    		if(grandChild.size() != 0 && grandParent.size() != 0){
    			for(int i=0;i<grandChild.size();i++){
    				for(int j=0;j<grandParent.size();j++){
    					context.write(new Text(grandChild.get(i)), new Text(grandParent.get(j)));
    				}
    			}
    		}
    	}
    }

```

### 4、	驱动实现 ###
---

驱动实现核心代码如下，详细源码请参考：SingletonTableJoin\src\com\zonesion\tablejoin\SingletonTableJoin.java。
```
    
    public static void main(String[] args) throws Exception {
    		Configuration conf = new Configuration();
    		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
    		if(otherArgs.length != 2){
    			System.err.println("Usage: SingletonTableJoin <in> <out>");
    		}
    		Job job = new Job(conf,"SingletonTableJoin Job");
    		job.setJarByClass(SingletonTableJoin.class);
    		job.setMapperClass(JoinMapper.class);
    		job.setReducerClass(JoinReducer.class);
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(Text.class);
    		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    		System.exit(job.waitForCompletion(true) ? 0 : -1);
    	}
```
### 5、部署运行 ###
---
#### 1）启动Hadoop集群 ####
```
    [hadoop@K-Master ~]$ start-dfs.sh
    [hadoop@K-Master ~]$ start-mapred.sh
    [hadoop@K-Master ~]$ jps
    5283 SecondaryNameNode
    5445 JobTracker
    5578 Jps
    5109 NameNode
```
#### 2）部署源码 ####
```
    #设置工作环境
    [hadoop@K-Master ~]$ mkdir -p /usr/hadoop/workspace/MapReduce
    #部署源码
    将SingletonTableJoin 文件夹拷贝到/usr/hadoop/workspace/MapReduce/ 路径下；
```
… 你可以直接 [下载 SingletonTableJoin](http://pan.baidu.com/s/1kTzKWej)  

	
#### 3）编译文件 ####
```
    #切换工作目录
    [hadoop@K-Master ~]$ cd /usr/hadoop/workspace/MapReduce/SingletonTableJoin
    #编译文件
    [hadoop@K-Master SingletonTableJoin]$ javac -classpath /usr/hadoop/hadoop-core-1.2.1.jar:/usr/hadoop/lib/commons-cli-1.2.jar -d bin src/com/zonesion/tablejoin/SingletonTableJoin.java
    #查看编译文件
    [hadoop@K-Master SingletonTableJoin]$ ls -la bin/com/zonesion/tablejoin/
    总用量 12
    drwxrwxr-x 2 hadoop hadoop  122 7月  31 11:02 .
    drwxrwxr-x 3 hadoop hadoop   22 7月  31 11:02 ..
    -rw-rw-r-- 1 hadoop hadoop 1856 7月  31 11:02 SingletonTableJoin.class
    -rw-rw-r-- 1 hadoop hadoop 2047 7月  31 11:02 SingletonTableJoin$JoinMapper.class
    -rw-rw-r-- 1 hadoop hadoop 2074 7月  31 11:02 SingletonTableJoin$JoinReducer.class
```
#### 4）打包jar文件 ####
```
    [hadoop@K-Master SingletonTableJoin]$ jar -cvf SingletonTableJoin.jar -C bin/ .
    added manifest
    adding: com/(in = 0) (out= 0)(stored 0%)
    adding: com/zonesion/(in = 0) (out= 0)(stored 0%)
    adding: com/zonesion/tablejoin/(in = 0) (out= 0)(stored 0%)
    adding: com/zonesion/tablejoin/SingletonTableJoin$JoinReducer.class(in = 2217) (out= 1006)(deflated 54%)
    adding: com/zonesion/tablejoin/SingletonTableJoin$JoinMapper.class(in = 1946) (out= 823)(deflated 57%)
    adding: com/zonesion/tablejoin/SingletonTableJoin.class(in = 1856) (out= 955)(deflated 48%)
```
#### 5）上传输入文件 ####
```
    [hadoop@K-Master SingletonTableJoin]$ hadoop fs -mkdir /user/hadoop/SingletonTableJoin/input/
    [hadoop@K-Master SingletonTableJoin]$ hadoop fs -put input/file01.txt  /user/hadoop/SingletonTableJoin/input/
    [hadoop@K-Master SingletonTableJoin]$ hadoop fs -ls /user/hadoop/SingletonTableJoin/input/ 
    Found 1 items
    -rw-r--r--   1 hadoop supergroup163 2014-07-31 11:08 /user/hadoop/SingletonTableJoin/input/file01.txt
```  
#### 6）运行Jar文件 ####
```
    [hadoop@K-Master SingletonTableJoin]$ hadoop jar SingletonTableJoin.jar com.zonesion.tablejoin.SingletonTableJoin SingletonTableJoin/input SingletonTableJoin/output
    
    14/07/31 14:47:55 INFO input.FileInputFormat: Total input paths to process : 1
    14/07/31 14:47:55 INFO util.NativeCodeLoader: Loaded the native-hadoop library
    14/07/31 14:47:55 WARN snappy.LoadSnappy: Snappy native library not loaded
    14/07/31 14:47:56 INFO mapred.JobClient: Running job: job_201407310921_0012
    14/07/31 14:47:57 INFO mapred.JobClient:  map 0% reduce 0%
    14/07/31 14:48:00 INFO mapred.JobClient:  map 100% reduce 0%
    14/07/31 14:48:07 INFO mapred.JobClient:  map 100% reduce 33%
    14/07/31 14:48:08 INFO mapred.JobClient:  map 100% reduce 100%
    14/07/31 14:48:08 INFO mapred.JobClient: Job complete: job_201407310921_0012
    .....
```

特别注意：在指定主类时，一定要使用完整包名com.zonesion.tablejoin.SingletonTableJoin，不然提示找不到。

#### 7）查看输出结果 ####
```
    [hadoop@K-Master SingletonTableJoin]$ hadoop fs -cat SingletonTableJoin/output/part-r-00000
    Tom		Alice
    Tom		Jesse
    Jone	Alice
    Jone	Jesse
    Tom		Mary
    Tom		Ben
    Jone	Mary
    Jone	Ben
    Philip	Alice
    Philip	Jesse
    Mark	Alice
    Mark	Jesse
```