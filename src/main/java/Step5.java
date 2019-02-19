import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
//import HDFSAPI;

public class Step5 {
	public static class Step5_FilterSortMapper extends Mapper<LongWritable, Text, Text, Text> {
		private String flag;
        private final static Text k = new Text();
        private final static Text v = new Text();

		@Override
		protected void setup(
				Context context)
				throws IOException, InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			flag = split.getPath().getParent().getName();// dataset
		}
        //Input: ItemId_A:UserId -> SUM(Count*ItemScore)
        //Output: UserId -> ItemId_A:SUM(Count*ItemScore)
		@Override
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			//you can use provided SortHashMap.java or design your own code.
			if(flag.equalsIgnoreCase("step4_2")){
                String[] tokens = Recommend.DELIMITER.split(values.toString());
                String[] itemUser = tokens[0].split(":");
                k.set(itemUser[1]);
                v.set(itemUser[0]+":"+tokens[1]);
                context.write(k,v);
			}
		}
	}
	public static class Step5_FilterSortReducer extends Reducer<Text, Text, Text, Text> {
        private final static Text v = new Text();
        //Input: UserId -> ItemId_A:SUM(Count*ItemScore)
        //Output: UserId -> ItemId_A:SUM(Count*ItemScore),ItemId_B:SUM(Count*ItemScore)
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	//you can use provided SortHashMap.java or design your own code.
            int userId = 153; //hardcoded from e0338153 last 3 digits
            int keyUserId = Integer.parseInt(key.toString());
            if(userId != keyUserId){
                return;
            }
            HashMap<String,Float> itemRatings = new HashMap<String, Float>();
            for(Text value : values){
                String [] itemRating = value.toString().split(":");
                String itemId = itemRating[0];
                Float rating = Float.parseFloat(itemRating[1]);
                itemRatings.put(itemId,rating);
            }
            List<Map.Entry<String,Float>> sortedItemRatings = SortHashMap.sortHashMap(itemRatings);
            for(Map.Entry<String,Float> entry : sortedItemRatings){
                v.set(entry.getKey()+":"+entry.getValue());
                context.write(key,v);
            }
//            StringBuilder sb = new StringBuilder();
//            for (Text value:values) {
//                sb.append("," + value.toString());
//            }
//            v.set(sb.toString().replaceFirst(",", ""));
//            context.write(key, v);

        }
    }
	public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
		//get configuration info
		Configuration conf = Recommend.config();
		// I/O path
		Path input1 = new Path(path.get("Step5Input1"));
		Path input2 = new Path(path.get("Step5Input2"));
		Path output = new Path(path.get("Step5Output"));
		// delete last saved output
		HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
		hdfs.delFile(output);
		// set job
        Job job =Job.getInstance(conf);
        job.setJarByClass(Step5.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step5_FilterSortMapper.class);
        job.setReducerClass(Step5_FilterSortReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, input1,input2);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
	}
}

