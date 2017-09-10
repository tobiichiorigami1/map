import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MyWordCount {

	/**
	 * @param args
	 */
	public static class MyMapper extends Mapper<Object,Text,Text,IntWritable>{
         public   final static IntWritable one=new IntWritable(1);
         private static Text word=new Text();
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			StringTokenizer str=new StringTokenizer(value.toString());
//			String[]str=new String(value.toString()).split("a");
			while(str.hasMoreTokens()){
				word.set(str.nextToken());
				context.write(word, one);
				}
//			for(String b:str){
//				word.set(b);
//				context.write(word, one);
//				
//			}
			
		}
	     
	}
	public static class MyReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		 private IntWritable result=new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
			Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum=0;
			for(IntWritable a:values){
				sum+=a.get();
				}
			result.set(sum);
			context.write(key, result);
		}
		 
		
	}
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stubn
         Configuration conf=new Configuration();
         FileSystem hdfs=FileSystem.get(conf);
         System.out.println(conf.get("fs.defaultFS"));
         //conf.set("fs.defaultFS","http//:localhost:9000");
         Job job=new Job(conf,"word cout");
         job.setJarByClass(MyWordCount.class);
         job.setMapperClass(MyMapper.class);
         job.setReducerClass(MyReduce.class);
         job.setCombinerClass(MyReduce.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(IntWritable.class);
//         job.setInputFormatClass(TextInputFormat.class);
//         job.setOutputFormatClass(TextOutputFormat.class);
         FileInputFormat.addInputPath(job, new Path("file:///usr/local/hadoop-2.6.1/input"));
         FileOutputFormat.setOutputPath(job, new Path("hdfs:///user/output"));
         System.exit(job.waitForCompletion(true) ? 0 : 1);
         }

}
