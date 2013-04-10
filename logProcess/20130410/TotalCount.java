package dataMining.logProcess;

import java.io.IOException;
import java.util.*;

//配置文件的类  访问hadoop分布式系统架构的配置文件
import org.apache.hadoop.conf.Configuration;

//运行HDFS所必须的类
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

//MAPREDUCE并行框架所需要的类
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



//数据抽样。
public class TotalCount  {

	
public static class Map extends Mapper<LongWritable, Text, Text,IntWritable >{
		
		private Text text = new Text("1");
		
		public void map(LongWritable key,Text value,Context context) throws IOException ,InterruptedException{
			
			String[] line = value.toString().split("\t");
			//从后向前搜索第二个引号，然后获取字串
			
			context.write(text, new IntWritable(Integer.parseInt(line[1])));
		}
	}
	



	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key,Iterator<IntWritable> values,Context context)throws IOException ,InterruptedException {
			int count = 0;
			while (values.hasNext()) {
				IntWritable intWritable = (IntWritable) values.next();
				count+= intWritable.get();
			}
			
			context.write(new Text("totalNumber"), new IntWritable(count));
		}
	} 
	
	public static void main(String[] args) throws Exception{
	
		if(args.length!=2){
			throw new Exception("输入参数错误！");
		}
		
		Path inputPath = new Path(args[0]);
		Path outPutPath = new Path(args[1]);
		
		//获取系统默认配置
		Configuration configuration = new Configuration();
		
		Job job = new Job(configuration);
		
		
		job.setJarByClass(TotalCount.class);
		job.setJobName("helloWorld!");
		
		
		//设置map  combine  reduce 工作类
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		
		//设置输出的key/value对类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		//当MapReduce输出数据到文件时，使用outPutFormat类，默认为TextOutputFormatd
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//hadoop分割和读取文件的的方式被定义在inputFormat接口的一个实现中，换句话说，
		//inputFormat使用来产生可供map处理的key\value对
		job.setInputFormatClass(TextInputFormat.class);
		
		
		//文件的输入输出路径
		FileInputFormat.setInputPaths(job,inputPath);
		FileOutputFormat.setOutputPath(job, outPutPath);
		
		boolean result = job.waitForCompletion(true);
		if (result) {
			System.out.println("WorkDone");
		}
		else {
			System.out.println("WorkFail");
		}

	}

}










