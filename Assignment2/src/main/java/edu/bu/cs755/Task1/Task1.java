package edu.bu.cs755.Task1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


// successfully returns the number of trips taken in an hour per day
public class Task1 {

    public static class GetErrors extends Mapper<Object, Text, Text, IntWritable>{

        // Set the variable which will be the value in the output map
    	// Create a counter and initialize with 1
    	
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // check if the distance values are 0
            if  (fields.length == 17) {
                if (fields[6].equals("0.000000") || fields[7].equals("0.000000") || fields[8].equals("0.000000") || fields[9].equals("0.000000") || 
                	fields[6].equals("") || fields[7].equals("") || fields[8].equals("") || fields[9].equals("")) {
                    context.write(new Text(fields[2].substring(11, 13)), one);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job =  new Job(conf, "task1");
        
        job.setJarByClass(Task1.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        /*  * Map - Map by node name
		- (A, 3) (D, 3) (A, 2) (B, 2) (A, 1) (C, 1) (A, 6) (F, 6) ...
         */
        job.setMapperClass(GetErrors.class);
        
        /*  * Shuffle - aggregate by key
		- [(A, [3, 2, 1, 6,...]) (B, [2,...]) (C, [1,...]) (D, [3,...]) ]
         */
        job.setCombinerClass(IntSumReducer.class);
        
        /*
         * Reduce - sum by key
		- [(A, 12) (B, 6) (D, 19) ...]
         */
        job.setReducerClass(IntSumReducer.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
