
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
//import HDFSAPI;

public class Step4_1 {
    public static class Step4_PartialMultiplyMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final static Text k = new Text();
        private final static Text v = new Text();
        // you can solve the co-occurrence Matrix/left matrix and score matrix/right matrix separately
        String flag;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();// data set
        }
        //Input: ItemId_A:ItemId_B -> Count //Input: UserId -> ItemId:ItemScore
        //Output: ItemId_B -> step3_2:ItemId_A:Count
        //Output: ItemId -> step3_1:UserId:ItemScore
        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
          //  System.out.println("inside map 4_1"+flag);
                if (flag.equalsIgnoreCase("step3_2")) {
                    String[] items = tokens[0].split(":");
                    k.set(items[1]);
                    v.set("step3_2" + ":" + items[0] + ":" + tokens[1]);
                    context.write(k, v);
                } else if (flag.equalsIgnoreCase("step3_1")) {
                    k.set(tokens[0]);
                    v.set("step3_1" + ":" + tokens[1]);
                    context.write(k, v);
                }
        }
    }

        public static class Step4_AggregateReducer extends Reducer<Text, Text, Text, Text> {
            private final static Text v = new Text();
            //Input: ItemId_B -> step3_2:ItemId_A:Count or ItemID -> step3_1:UserId:ItemScore
            //Output: ItemId_B -> ItemId_A:UserId:Count*ItemScore
            @Override
            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                List<String> values_Step3_1 = new ArrayList<String>();
                List<String> values_step3_2 = new ArrayList<String>();

                for(Text value : values){
                    String [] tokens = value.toString().split(":");
                    if(tokens[0].equalsIgnoreCase("step3_1")){
                        values_Step3_1.add(tokens[1]+":"+tokens[2]);
                    }else if(tokens[0].equalsIgnoreCase("step3_2")){
                        values_step3_2.add(tokens[1]+":"+tokens[2]);
                    }
                }
            Iterator<String> iterator3_1 = values_Step3_1.iterator();
                while(iterator3_1.hasNext()){
                    String [] tokens_3_1 = iterator3_1.next().split(":");

                    Iterator<String> iterator3_2 = values_step3_2.iterator();
                    while (iterator3_2.hasNext()){
                        String [] tokens_3_2 = iterator3_2.next().split(":");
                        double countScore = Double.parseDouble(tokens_3_1[1])*Double.parseDouble(tokens_3_2[1]);
                        v.set(tokens_3_2[0]+":"+tokens_3_1[0]+":"+countScore);
                        context.write(key,v);
                    }
                }

            }
        }

        public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
            //get configuration info
            Configuration conf = Recommend.config();
            // get I/O path
            Path input1 = new Path(path.get("Step4_1Input1"));
            Path input2 = new Path(path.get("Step4_1Input2"));
            Path output = new Path(path.get("Step4_1Output"));
            // delete last saved output
            HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
            hdfs.delFile(output);
            // set job
            Job job = Job.getInstance(conf);
            job.setJarByClass(Step4_1.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(Step4_PartialMultiplyMapper.class);
            job.setReducerClass(Step4_AggregateReducer.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job, input1, input2);
            FileOutputFormat.setOutputPath(job, output);

            job.waitForCompletion(true);
        }
}

