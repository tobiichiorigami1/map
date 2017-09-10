import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Task.CombinerRunner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MineMapper {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static class MiMapper extends Mapper<Object,Text,Text,IntWritable>{
		public final static IntWritable one=new IntWritable(1);
		Text word=new Text();
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		StringTokenizer str=new StringTokenizer(value.toString());
		while (str.hasMoreElements()) {
			 word.set(str.nextToken());
			 context.write(word, one);
			
		}
		}
		
	}
	public static class MyCome extends Reducer<Text,IntWritable,Text,IntWritable>{
		                   IntWritable as=new IntWritable();
		                   @Override
		                protected void reduce(Text a, Iterable<IntWritable> vd,
		                Context ag)
		                throws IOException, InterruptedException {
		                // TODO Auto-generated method stub
		                int sum=0;
		                for(IntWritable h:vd){
		                	sum+=h.get();
		                }
		                as.set(sum);
		                ag.write(a, as);
		                }
		
	}
	public static class Mireduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable result=new IntWritable();

		@Override
		protected void reduce(Text a, Iterable<IntWritable> var,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		    int sum=0;      	
			for(IntWritable y:var){
		          		sum+=y.get();
		          	}
			result.set(sum);
	           context.write(a, result);        		     
		}
		
		
		
	} 
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
        Configuration conf=new Configuration();
        Job job=new Job(conf,"mapper");
        job.setJarByClass(Mapper.class);
        job.setMapperClass(MiMapper.class);
        job.setReducerClass(Mireduce.class);
        job.setCombinerClass(MyCome.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("file:///usr/local/hadoop-2.6.1/input"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs:///user/output"));
        System.exit(job.waitForCompletion(true)?1:0);
	}

}
