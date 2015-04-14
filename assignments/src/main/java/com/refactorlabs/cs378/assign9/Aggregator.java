package com.refactorlabs.cs378.assign9;

import com.refactorlabs.cs378.sessions.EventSubtype;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.commons.collections.map.DefaultedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

import static com.refactorlabs.cs378.utils.Utils.printClassPath;

/**
 * MapReduce program to collect word statistics (per paragraph in the input document).
 * <p/>
 * Removes punctuation and maps all words to lower case.
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class Aggregator extends Configured implements Tool {

    /**
     * The Map class for word statistics.  Extends class Mapper, provided by Hadoop.
     * This class defines the map() function for the word statistics example.
     */
    public static class AggregatorMapClass extends Mapper<AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>,
            AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>> {

        @Override
        public void map(AvroKey<ClickSubtypeStatisticsKey> key, AvroValue<ClickSubtypeStatisticsData> value, Context context)
                throws IOException, InterruptedException {

            CharSequence sessionType = key.datum().getSessionType();
            CharSequence clickSubType = key.datum().getClickSubtype();

            context.write(new AvroKey<ClickSubtypeStatisticsKey>(key.datum()),
                    new AvroValue<ClickSubtypeStatisticsData>(value.datum()));

            ClickSubtypeStatisticsKey.Builder keyBuilder = ClickSubtypeStatisticsKey.newBuilder();

            keyBuilder.setSessionType("any");
            keyBuilder.setClickSubtype(clickSubType);
            context.write(new AvroKey<ClickSubtypeStatisticsKey>(keyBuilder.build()),
                    new AvroValue<ClickSubtypeStatisticsData>(value.datum()));

            ClickSubtypeStatisticsData.Builder data = ClickSubtypeStatisticsData.newBuilder(value.datum());
            if(!clickSubType.equals(EventSubtype.ALTERNATIVE.toString()))
                data.setSessionCount(0);

            keyBuilder.setSessionType(sessionType);
            keyBuilder.setClickSubtype("any");
            context.write(new AvroKey<ClickSubtypeStatisticsKey>(keyBuilder.build()),
                    new AvroValue<ClickSubtypeStatisticsData>(data.build()));

            keyBuilder.setSessionType("any");
            keyBuilder.setClickSubtype("any");
            context.write(new AvroKey<ClickSubtypeStatisticsKey>(keyBuilder.build()),
                    new AvroValue<ClickSubtypeStatisticsData>(data.build()));
        }

    }

    /**
     * The Reduce class for word statistics.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word statistics example.
     */
    public static class AggregatorReducerClass extends Reducer<AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>,
            AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>> {

        @Override
        public void reduce(AvroKey<ClickSubtypeStatisticsKey> key, Iterable<AvroValue<ClickSubtypeStatisticsData>> values, Context context)
                throws IOException, InterruptedException {

            long session_count = 0l; long total_count = 0l; long sos = 0l;
            for(AvroValue<ClickSubtypeStatisticsData> value: values) {
                ClickSubtypeStatisticsData data = value.datum();
                session_count += data.getSessionCount();
                total_count += data.getTotalCount();
                sos += data.getSumOfSquares();
            }

            ClickSubtypeStatisticsData.Builder builder = ClickSubtypeStatisticsData.newBuilder();
            builder.setSessionCount(session_count);
            builder.setTotalCount(total_count);
            builder.setSumOfSquares(sos);

            double mean = session_count != 0 ? (double)total_count / (double)session_count : 0;
            double variance = session_count != 0 ? ((double)sos / session_count - mean * mean) : 0;

            builder.setMean(mean);
            builder.setVariance(variance);

            context.write(key, new AvroValue<ClickSubtypeStatisticsData>(builder.build()));
        }
    }


    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Aggregator <input> <output>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = new Job(conf, "Aggregator");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(Aggregator.class);
        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.set("mapreduce.user.classpath.first", "true");


        // Specify the Map
        job.setMapperClass(AggregatorMapClass.class);
        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, ClickSubtypeStatisticsKey.getClassSchema());
        AvroJob.setInputValueSchema(job, ClickSubtypeStatisticsData.getClassSchema());
        AvroJob.setMapOutputKeySchema(job, ClickSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(job, ClickSubtypeStatisticsData.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(AggregatorReducerClass.class);
        AvroJob.setOutputKeySchema(job, ClickSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(job, ClickSubtypeStatisticsData.getClassSchema());
        job.setOutputValueClass(AvroValue.class);

        // Initiate the map-reduce job, and wait for completion.
        String[] inputPaths = appArgs[0].split(";");
        for (String inputPath : inputPaths) {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1] + "_aggregator"));
        job.waitForCompletion(true);

        return 0;
    }
    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        printClassPath();
        int res = ToolRunner.run(new Configuration(), new Aggregator(), args);
        System.exit(res);
    }

}
