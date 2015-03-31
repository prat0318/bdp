package com.refactorlabs.cs378.assign7;

import com.refactorlabs.cs378.sessions.*;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.commons.collections.map.DefaultedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
public class ReduceJoin extends Configured implements Tool {

    /**
     * The Map class for word statistics.  Extends class Mapper, provided by Hadoop.
     * This class defines the map() function for the word statistics example.
     */
    public static class SessionMapClass extends Mapper<AvroKey<Session>, NullWritable, AvroKey<CharSequence>, AvroValue<VinImpressionCounts>> {

        @Override
        public void map(AvroKey<Session> value, NullWritable _, Context context)
                throws IOException, InterruptedException {

            List<Event> events = value.datum().getEvents();
            Set<CharSequence> uniques = new HashSet<CharSequence>();
            Set<CharSequence> submitContactForm = new HashSet<CharSequence>();
            Set<CharSequence> shareMarketReport = new HashSet<CharSequence>();
            Map<EventSubtype, Set<CharSequence>> clicks = new HashMap<EventSubtype, Set<CharSequence>>();

            for (Event event: events) {
                EventType eventType = event.getEventType();
                EventSubtype eventSubtype = event.getEventSubtype();
                CharSequence vin = event.getVin();
                AvroKey<CharSequence> vinKey = new AvroKey<CharSequence>(vin);
                VinImpressionCounts.Builder builder = VinImpressionCounts.newBuilder();
                builder.setUniqueUser(0L);
                builder.setSubmitContactForm(0L);
                builder.setShareMarketReport(0L);

                if(!uniques.contains(vin)) {
                    builder.setUniqueUser(1L);
                    uniques.add(vin);
                }
                if(eventType.equals(EventType.SUBMIT) && eventSubtype.equals(EventSubtype.CONTACT_FORM) &&
                        !submitContactForm.contains(vin)) {
                    builder.setSubmitContactForm(1L);
                    submitContactForm.add(vin);
                }
                if(eventType.equals(EventType.SHARE) && eventSubtype.equals(EventSubtype.MARKET_REPORT) &&
                        !shareMarketReport.contains(vin)) {
                    builder.setShareMarketReport(1l);
                    shareMarketReport.add(vin);
                }
                if(eventType.equals(EventType.CLICK)) {
                    if(!(clicks.containsKey(eventSubtype) && clicks.get(eventSubtype).contains(vin))) {
                        Map<CharSequence, Long> click = new HashMap<CharSequence, Long>();
                        click.put(eventSubtype.name(), 1L);
                        builder.setClicks(click);
                        if(!(clicks.containsKey(eventSubtype)))
                            clicks.put(eventSubtype, new HashSet<CharSequence>());
                        clicks.get(eventSubtype).add(vin);
                    }
                }
                context.write(vinKey, new AvroValue<VinImpressionCounts>(builder.build()));
            }

        }

    }

    public static class VinMapClass extends Mapper<LongWritable, Text, AvroKey<CharSequence>, AvroValue<VinImpressionCounts>> {

        @Override
        public void map(LongWritable _key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] lineArr = line.split(",");
            if(lineArr[2].equals("count")) return;
            String vin = lineArr[0];
            AvroKey<CharSequence> vinKey = new AvroKey<CharSequence>(vin);
            VinImpressionCounts.Builder builder = VinImpressionCounts.newBuilder();
            builder.setUniqueUserVdpView(Long.parseLong(lineArr[2]));
            context.write(vinKey, new AvroValue<VinImpressionCounts>(builder.build()));
        }

    }

    public static class SharedValues {
        public Map<CharSequence, Long> clicks;
        public long[] values;
    }

    public static SharedValues getSharedValues(Iterable<AvroValue<VinImpressionCounts>> values) {
        long uniques = 0L; long share_market_report = 0L; long submit_contact_form = 0L;
        long unique_vdp = 0L;

        SharedValues sharedValues = new SharedValues();
        sharedValues.clicks = new DefaultedMap(0L);

        for(AvroValue<VinImpressionCounts> value: values) {
            uniques += value.datum().getUniqueUser();
            share_market_report += value.datum().getShareMarketReport();
            submit_contact_form += value.datum().getSubmitContactForm();
            unique_vdp += value.datum().getUniqueUserVdpView();
            if(value.datum().getClicks() != null) {
                for (Map.Entry<CharSequence, Long> entry : value.datum().getClicks().entrySet()) {
                    sharedValues.clicks.put(entry.getKey(),
                            sharedValues.clicks.get(entry.getKey()) + entry.getValue());
                }
            }
        }
        sharedValues.values = new long[]{uniques, share_market_report, submit_contact_form, unique_vdp};
        return sharedValues;
    }
    /**
     * The Combiner class for word statistics.  Extends class Reducer, provided by Hadoop.
     * This class defines a reduce() method, for combining (summing) map output in the word statistics job.
     * All the work is done by helper methods on the class LongArrayWritable.
     */
    public static class CombinerClass extends Reducer<AvroKey<CharSequence>, AvroValue<VinImpressionCounts>,
            AvroKey<CharSequence>, AvroValue<VinImpressionCounts>> {

        @Override
        public void reduce(AvroKey<CharSequence> key, Iterable<AvroValue<VinImpressionCounts>> values, Context context)
                throws IOException, InterruptedException {
            SharedValues combined_values = getSharedValues(values);

            long uniques = combined_values.values[0];
            long share_market_report = combined_values.values[1];
            long submit_contact_form = combined_values.values[2];
            long unique_vdp = combined_values.values[3];
            Map<CharSequence, Long> clicks = combined_values.clicks;

            VinImpressionCounts.Builder builder = VinImpressionCounts.newBuilder();
            builder.setUniqueUser(uniques);
            builder.setShareMarketReport(share_market_report);
            builder.setSubmitContactForm(submit_contact_form);
            builder.setUniqueUserVdpView(unique_vdp);
            builder.setClicks(clicks);
            AvroKey<CharSequence> vinKey = new AvroKey<CharSequence>(key.toString());
            context.write(vinKey, new AvroValue<VinImpressionCounts>(builder.build()));
        }
    }

    /**
     * The Reduce class for word statistics.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word statistics example.
     */
    public static class ReduceClass extends Reducer<AvroKey<CharSequence>, AvroValue<VinImpressionCounts>,
            AvroKey<Pair<CharSequence, VinImpressionCounts>>, NullWritable> {



        @Override
        public void reduce(AvroKey<CharSequence> key, Iterable<AvroValue<VinImpressionCounts>> values, Context context)
                throws IOException, InterruptedException {
            SharedValues combined_values = getSharedValues(values);

            long uniques = combined_values.values[0];
            long share_market_report = combined_values.values[1];
            long submit_contact_form = combined_values.values[2];
            long unique_vdp = combined_values.values[3];
            Map<CharSequence, Long> clicks = combined_values.clicks;

            if(uniques == 0) return;
            VinImpressionCounts.Builder builder = VinImpressionCounts.newBuilder();
            builder.setUniqueUser(uniques);
            builder.setSubmitContactForm(submit_contact_form);
            builder.setShareMarketReport(share_market_report);
            builder.setUniqueUserVdpView(unique_vdp);
            builder.setClicks(clicks);
            context.write(
                    new AvroKey<Pair<CharSequence, VinImpressionCounts>>
                            (new Pair<CharSequence, VinImpressionCounts>(key.toString(), builder.build())),
                    NullWritable.get());
        }
    }


    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ReduceJoin <input path1> <input path2> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = new Job(conf, "ReduceJoin");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(ReduceJoin.class);
        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.set("mapreduce.user.classpath.first", "true");

        // Specify the Map
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setMapperClass(MapClass.class);
//        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setMapOutputValueSchema(job, VinImpressionCounts.getClassSchema());
        AvroJob.setInputKeySchema(job, Session.getClassSchema());
        MultipleInputs.addInputPath(job, new Path(appArgs[0]), AvroKeyInputFormat.class, SessionMapClass.class);
        MultipleInputs.addInputPath(job, new Path(appArgs[1]), TextInputFormat.class, VinMapClass.class);

        //Specify the Combiner
        job.setCombinerClass(CombinerClass.class);

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        AvroJob.setOutputKeySchema(job,
                Pair.getPairSchema(Schema.create(Schema.Type.STRING), VinImpressionCounts.getClassSchema()));
        job.setOutputValueClass(NullWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(appArgs[2]));

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
        int res = ToolRunner.run(new Configuration(), new ReduceJoin(), args);
        System.exit(res);
    }

}
