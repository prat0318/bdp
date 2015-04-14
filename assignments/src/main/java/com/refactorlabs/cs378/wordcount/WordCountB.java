package com.refactorlabs.cs378.wordcount;

import com.refactorlabs.cs378.utils.Utils;
import com.refactorlabs.cs378.wordcount.WordCountData;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * WordCount example using Avro defined class for the word count data,
 * to demonstrate how to use Avro defined objects.
 * <p/>
 * Here the reduce outputs (key and value) are both Avro objects, written as text.
 */
public class WordCountB extends Configured implements Tool {

    /**
     * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word count example.
     */
    public static class ReduceClass
            extends Reducer<Text, AvroValue<WordCountData>, AvroKey<CharSequence>, AvroValue<WordCountData>> {

        @Override
        public void reduce(Text key, Iterable<AvroValue<WordCountData>> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0L;

            context.getCounter(Utils.REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

            // Sum up the counts for the current word, specified in object "key".
            for (AvroValue<WordCountData> value : values) {
                sum += value.datum().getCount();
            }
            // Emit the total count for the word.
            WordCountData.Builder builder = WordCountData.newBuilder();
            builder.setCount(sum);
            context.write(new AvroKey<CharSequence>(key.toString()), new AvroValue<WordCountData>(builder.build()));
        }
    }

    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCountB <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = new Job(conf, "WordCountB");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(WordCountB.class);
        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.set("mapreduce.user.classpath.first", "true");

        // Specify the Map
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, WordCountData.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, WordCountData.getClassSchema());

        // Grab the input file and output directory from the command line.
        String[] inputPaths = appArgs[0].split(",");
        for (String inputPath : inputPaths) {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

        return 0;
    }

    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        Utils.printClassPath();
        int res = ToolRunner.run(new Configuration(), new WordCountB(), args);
        System.exit(res);
    }

}
