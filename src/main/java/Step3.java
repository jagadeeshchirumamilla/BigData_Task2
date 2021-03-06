
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import org.apache.hadoop.examples.HDFSAPI;

public class Step3 {
	public static class Step31_UserVectorSplitterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		private final static IntWritable k = new IntWritable();
		private final static Text v = new Text();
		//Input:  userId -> itemId:itemScore,itemId:itemScore ,.....
		//Output: itemId -> userId:itemScore
		@Override
		public void map(LongWritable key, Text values, Context context)
				throws IOException, InterruptedException {
			String[] tokens = Recommend.DELIMITER.split(values.toString());
            for(int i=1;i<tokens.length;i++){
                String[] itemScore = tokens[i].split(":");
                String userId = tokens[0];
                String score = itemScore[1];
                int itemId = Integer.parseInt(itemScore[0]);
                k.set(itemId);
                v.set(userId+":"+score);
                context.write(k,v);
            }
		}
	}
	public static void run1(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
		//get configuration info
    	Configuration conf = Recommend.config();
    	//get I/O path
        Path input = new Path(path.get("Step3Input1"));
        Path output = new Path(path.get("Step3Output1"));
        //delete the last saved output
        HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
        hdfs.delFile(output);
        //set job
        Job job =Job.getInstance(conf,"Step3_1");
		job.setJarByClass(Step3.class);
		
		job.setMapperClass(Step31_UserVectorSplitterMapper.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,input);
		FileOutputFormat.setOutputPath(job,output);
		//run job
		job.waitForCompletion(true);
	}
	
	 public static class Step32_CooccurrenceColumnWrapperMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static Text k = new Text();
		private final static IntWritable v = new IntWritable();
        //iteamA:itemB value
		 //Input: itemId_A:itemId_B -> sum
		//Output: itemId_A:itemId_B -> sum
		@Override
		public void map(LongWritable key, Text values,Context context)
				throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
				String itemAB = tokens[0].toString();
				int sum = Integer.parseInt(tokens[1].toString());
                k.set(itemAB);
                v.set(sum);
                context.write(k,v);
		}
	}

	public static void run2(Map<String, String> path) throws IOException,
		ClassNotFoundException, InterruptedException {
		//get configuration info
		Configuration conf = Recommend.config();
		//get I/O path
		Path input = new Path(path.get("Step3Input2"));
		Path output = new Path(path.get("Step3Output2"));
		// delete the last saved output 
		HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
		hdfs.delFile(output);
		// set job
		Job job =Job.getInstance(conf, "Step3_2");
		job.setJarByClass(Step3.class);

		job.setMapperClass(Step32_CooccurrenceColumnWrapperMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		// run job
		job.waitForCompletion(true);
}
}

