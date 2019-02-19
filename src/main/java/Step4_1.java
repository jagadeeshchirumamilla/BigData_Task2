
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
        //Input: itemId -> userId:itemScore
        //Input: itemId_A:itemId_B -> sum
        //Output: itemId_B -> step3_2:itemId_A:sum
        //Output: itemId -> step3_1:userId:itemScore
        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
                if (flag.equalsIgnoreCase("step3_2")) {
                    String[] items = tokens[0].split(":");
                    String count = tokens[1];
                    String itemA = items[0];
                    String itemB = items[1];
                    k.set(itemB);
                    v.set("cooc_matrix" + ":" + itemA + ":" + count);
                    context.write(k, v);
                } else if (flag.equalsIgnoreCase("step3_1")) {
                    String itemId = tokens[0];
                    String userIdScore = tokens[1];
                    k.set(tokens[0]);
                    v.set("score_matrix" + ":" + userIdScore);
                    context.write(k, v);
                }
        }
    }

        public static class Step4_AggregateReducer extends Reducer<Text, Text, Text, Text> {
            private final static Text v = new Text();
            //Input: itemId_B -> step3_2:itemId_A:count or itemID -> step3_1:userId:score
            //Output: itemId_B -> itemId_A:userId:count*score
            @Override
            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                List<String> cooc_Matrix = new ArrayList<String>();
                List<String> score_Matrix = new ArrayList<String>();

                for(Text value : values){
                    String [] tokens = value.toString().split(":");
                    if(tokens[0].equalsIgnoreCase("score_matrix")){
                        score_Matrix.add(tokens[1]+":"+tokens[2]);
                    }else if(tokens[0].equalsIgnoreCase("cooc_matrix")){
                        cooc_Matrix.add(tokens[1]+":"+tokens[2]);
                    }
                }
            Iterator<String> score_iterator = score_Matrix.iterator();
                while(score_iterator.hasNext()){
                    String [] tokens_score = score_iterator.next().split(":");

                    Iterator<String> cooc_iterator = cooc_Matrix.iterator();
                    while (cooc_iterator.hasNext()){
                        String [] tokens_cooc = cooc_iterator.next().split(":");
                        double countScore = Double.parseDouble(tokens_score[1])*Double.parseDouble(tokens_cooc[1]);
                        v.set(tokens_cooc[0]+":"+tokens_score[0]+":"+countScore);
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

