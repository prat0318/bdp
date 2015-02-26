package com.refactorlabs.cs378.assign5;

import com.google.common.collect.Maps;
import com.refactorlabs.cs378.utils.DoubleArrayWritable;
import com.refactorlabs.cs378.utils.LongArrayWritable;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

import static com.refactorlabs.cs378.utils.Utils.printClassPath;

/**
 * MapReduce program to collect word statistics (per paragraph in the input document).
 * <p/>
 * Removes punctuation and maps all words to lower case.
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class WordStatistics extends Configured implements Tool {

    /**
     * The Map class for word statistics.  Extends class Mapper, provided by Hadoop.
     * This class defines the map() function for the word statistics example.
     */
    public static class MapClass extends Mapper<LongWritable, Text, Text, AvroValue<WordStatisticsData>> {

        private static final Integer INITIAL_COUNT = 1;

        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

        /**
         * Local variable "word" will contain a word identified in the input.
         * The Hadoop Text object is mutable, so we can reuse the same object and
         * simply reset its value as data for each word output.
         */
        private Text word = new Text();

        /**
         * Local variable "valueArray" will contain the values output for each word.
         * This object is mutable, so we can reuse the same object and
         * simply reset its value as data for each word output.
         */
        private LongArrayWritable valueArray = new LongArrayWritable();

        /**
         * Local variable writableValue is used to construct the array of Writable
         * values that will be added to valueArray.
         */
        private Writable[] writableValues = new Writable[3];

        @Override
        public void map(LongWritable _key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = standardize(value.toString());
            StringTokenizer tokenizer = new StringTokenizer(line);

            context.getCounter(MAPPER_COUNTER_GROUP, "Input Documents").increment(1L);

            Map<String, Integer> wordCountMap = Maps.newHashMap();
            // For each word in the input document, determine the number of times the
            // word occurs.  Keep the current counts in a hash map.
            while (tokenizer.hasMoreTokens()) {
                String nextWord = tokenizer.nextToken();
                Integer count = wordCountMap.get(nextWord);

                if (count == null) {
                    wordCountMap.put(nextWord, INITIAL_COUNT);
                } else {
                    wordCountMap.put(nextWord, count.intValue() + 1);
                }
            }

            // Create the output value for each word, and output the key/value pair.
            for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
                int count = entry.getValue().intValue();
                word.set(entry.getKey());
                WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
                builder.setDocumentCount(1L);
                builder.setTotalCount(count);
                builder.setSumOfSquares(count * count);
                builder.setMean(0.0);
                builder.setVariance(0.0);
                context.write(word, new AvroValue(builder.build()));
                context.getCounter(MAPPER_COUNTER_GROUP, "Output Words").increment(1L);
            }
        }

        /**
         * Remove punctuation and insert spaces where needed, so the tokenizer will identify words.
         */
        private String standardize(String input) {
            return StringUtils.replaceEach(input,
                    new String[]{".", ",", "\"", "_", "[", ";", "--", ":", "?", "!"},
                    new String[]{" ", " ", " ", " ", " [", " ", " ", " ", " ", " "}).toLowerCase();
        }
    }

    /**
     * The Combiner class for word statistics.  Extends class Reducer, provided by Hadoop.
     * This class defines a reduce() method, for combining (summing) map output in the word statistics job.
     * All the work is done by helper methods on the class LongArrayWritable.
     */
//    public static class CombinerClass extends Reducer<Text, LongArrayWritable, Text, LongArrayWritable> {
//
//        @Override
//        public void reduce(Text key, Iterable<LongArrayWritable> values, Context context)
//                throws IOException, InterruptedException {
//            context.write(key, LongArrayWritable.make(LongArrayWritable.sum(values)));
//        }
//    }

    /**
     * The Reduce class for word statistics.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word statistics example.
     */
    public static class ReduceClass extends Reducer<Text, AvroValue<WordStatisticsData>,
            AvroKey<Pair<CharSequence, WordStatisticsData>>, NullWritable> {

        /**
         * Counter group for the reducer.  Individual counters are grouped for the reducer.
         */
        private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

        @Override
        public void reduce(Text key, Iterable<AvroValue<WordStatisticsData>> values, Context context)
                throws IOException, InterruptedException {
            context.getCounter(REDUCER_COUNTER_GROUP, "Input Words").increment(1L);
            long docCount = 0L; long totCount = 0L; long sos = 0L;

            for(AvroValue<WordStatisticsData> value: values) {
                docCount += value.datum().getDocumentCount();
                totCount += value.datum().getTotalCount();
                sos += value.datum().getSumOfSquares();
            }

            WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
            builder.setDocumentCount(docCount);
            builder.setTotalCount(totCount);
            builder.setSumOfSquares(sos);
            double mean = (double) totCount / docCount;
            builder.setMean(mean);
            builder.setVariance((double) sos / docCount - mean * mean);
            context.write(
                    new AvroKey<Pair<CharSequence, WordStatisticsData>>
                            (new Pair<CharSequence, WordStatisticsData>(key.toString(), builder.build())),
                    NullWritable.get());
        }
    }


    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordStatistics <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = new Job(conf, "WordCountStatistics");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(WordStatistics.class);
        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.set("mapreduce.user.classpath.first", "true");

        // Specify the Map
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MapClass.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, WordStatisticsData.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        AvroJob.setOutputKeySchema(job,
                Pair.getPairSchema(Schema.create(Schema.Type.STRING), WordStatisticsData.getClassSchema()));
        job.setOutputValueClass(NullWritable.class);

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
        printClassPath();
        int res = ToolRunner.run(new Configuration(), new WordStatistics(), args);
        System.exit(res);
    }

}
