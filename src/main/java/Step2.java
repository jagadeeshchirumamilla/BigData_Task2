import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
//import org.apache.hadoop.examples.HDFSAPI;

public class Step2 {
    public static class Step2_UserVectorToCooccurrenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static Text k = new Text();
        private final static IntWritable v = new IntWritable(1);
        //Input: userId -> itemId:itemScore,itemId:itemScore,...
        //Output: itemId_A:itemId_B -> 1 & itemId_B:itemId_A -> 1
        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            //item by item matrix : first for loop fetching all itemIds
            //and then in second for loop itemA:itemB itemB:itemA is set to 1
            List<String> itemIds = new ArrayList<String>();
            for(int i=1;i<tokens.length;i++){
                String[] itemScore = tokens[i].split(":");
                itemIds.add(itemScore[0]);
            }
            for(int i=0;i<itemIds.size();i++){
                for(int j=i+1;j<itemIds.size();j++){
                    k.set(itemIds.get(i)+":"+itemIds.get(j));
                    context.write(k,v);
                    k.set(itemIds.get(j)+":"+itemIds.get(i));
                    context.write(k,v);
                }

            }
        }
    }


    public static class Step2_UserVectorToConoccurrenceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        //Input: itemId_A:itemId_B -> 1 & itemId_B:itemId_A -> 1
        // Output: itemId_A:itemId_B -> sum
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            //aggregating all the item counts
            int sum = 0;
            for(IntWritable value : values ){
                sum += value.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }
        public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
            //get configuration info
            Configuration conf = Recommend.config();
            //get I/O path
            Path input = new Path(path.get("Step2Input"));
            Path output = new Path(path.get("Step2Output"));
            //delete last saved output
            HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
            hdfs.delFile(output);
            //set job
            Job job = Job.getInstance(conf, "Step2");
            job.setJarByClass(Step2.class);

            job.setMapperClass(Step2_UserVectorToCooccurrenceMapper.class);
            job.setCombinerClass(Step2_UserVectorToConoccurrenceReducer.class);
            job.setReducerClass(Step2_UserVectorToConoccurrenceReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);
            //run job
            job.waitForCompletion(true);
        }

}

