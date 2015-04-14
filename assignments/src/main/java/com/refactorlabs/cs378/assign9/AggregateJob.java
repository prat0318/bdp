package com.refactorlabs.cs378.assign9;

import com.refactorlabs.cs378.sessions.Event;
import com.refactorlabs.cs378.sessions.EventSubtype;
import com.refactorlabs.cs378.sessions.EventType;
import com.refactorlabs.cs378.sessions.Session;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.*;
import org.apache.commons.collections.map.DefaultedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

/**
 * MapReduce program to collect word statistics (per paragraph in the input document).
 * <p/>
 * Removes punctuation and maps all words to lower case.
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class AggregateJob extends Configured implements Tool {

    /**
     * The Map class for word statistics.  Extends class Mapper, provided by Hadoop.
     * This class defines the map() function for the word statistics example.
     */

    public static class ClickerMapperClass extends ClickStatisticsMapperClass {

        @Override
        public String getSessionType() { return "Clicker"; }
    }

    public static class SharerMapperClass extends ClickStatisticsMapperClass {

        @Override
        public String getSessionType() { return "Sharer"; }
    }

    public static class SubmitterMapperClass extends ClickStatisticsMapperClass {

        @Override
        public String getSessionType() { return "Submitter"; }
    }

    public static class ClickStatisticsMapperClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>,
            AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>> {

        public String getSessionType() { return ""; }

        @Override
        public void map(AvroKey<CharSequence> _, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {
            List<Event> events = value.datum().getEvents();

            Map<EventSubtype, Long> cache = new DefaultedMap(0L);
            for (Event event: events) {
                EventType eventType = event.getEventType();
                EventSubtype eventSubtype = event.getEventSubtype();

                if(eventType.equals(EventType.CLICK))
                    cache.put(eventSubtype, cache.get(eventSubtype) + 1l);

            }

            for (EventSubtype subtype: EventSubtype.values()) {
                ClickSubtypeStatisticsKey.Builder keyBuilder = ClickSubtypeStatisticsKey.newBuilder();
                keyBuilder.setSessionType(getSessionType());
                keyBuilder.setClickSubtype(subtype.toString());

                ClickSubtypeStatisticsData.Builder dataBuilder = ClickSubtypeStatisticsData.newBuilder();
                dataBuilder.setSessionCount(1l);
                long total = cache.get(subtype);
                dataBuilder.setTotalCount(total);
                dataBuilder.setSumOfSquares(total * total);

                context.write(new AvroKey<ClickSubtypeStatisticsKey>(keyBuilder.build()),
                        new AvroValue<ClickSubtypeStatisticsData>(dataBuilder.build()));
            }
        }
    }

    public static class ClickStatisticsReducerClass extends Reducer<AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>,
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
        if (args.length != 4) {
            System.err.println("Usage: AggregateJob <input clicker> <input sharer> <input submitter> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        conf.set("mapreduce.user.classpath.first", "true");
        Job[] jobs = new Job[3];

        jobs[0] = getJob(conf, "clicker");
        jobs[0].setMapperClass(ClickerMapperClass.class);
        FileInputFormat.addInputPath(jobs[0], new Path(appArgs[0]));
        FileOutputFormat.setOutputPath(jobs[0], new Path(appArgs[3] + "_clicker"));

        /********************************************************/
        jobs[1] = getJob(conf, "sharer");
        jobs[1].setMapperClass(SharerMapperClass.class);
        FileInputFormat.addInputPath(jobs[1], new Path(appArgs[1]));
        FileOutputFormat.setOutputPath(jobs[1], new Path(appArgs[3] + "_sharer"));

        /********************************************************/
        jobs[2] = getJob(conf, "submitter");
        jobs[2].setMapperClass(SubmitterMapperClass.class);
        FileInputFormat.addInputPath(jobs[2], new Path(appArgs[2]));
        FileOutputFormat.setOutputPath(jobs[2], new Path(appArgs[3] + "_submitter"));

        for(int i = 0; i < 3; i++) { jobs[i].submit(); }

        boolean isAllFinished = false;
        while(!isAllFinished) {
            isAllFinished = jobs[0].isComplete() & jobs[1].isComplete() & jobs[2].isComplete();
            Thread.sleep(1000);
        }

        return 0;
    }

    public Job getJob(Configuration conf, String type) throws IOException {

        Job job1 = new Job(conf, "AggregateJob_" + type);
        job1.setJarByClass(AggregateJob.class);

        // Specify the Map
        job1.setInputFormatClass(AvroKeyValueInputFormat.class);
//        job1.setMapperClass(ClickStatisticsMapperClass.class);
        job1.setMapOutputKeyClass(Text.class);
        AvroJob.setInputKeySchema(job1, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job1, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(job1, ClickSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(job1, ClickSubtypeStatisticsData.getClassSchema());

        // Specify the Reduce
        job1.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job1.setReducerClass(ClickStatisticsReducerClass.class);
        AvroJob.setOutputKeySchema(job1, ClickSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(job1, ClickSubtypeStatisticsData.getClassSchema());
        job1.setOutputValueClass(AvroValue.class);

        return job1;
    }

    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AggregateJob(), args);
        System.exit(res);
    }

}

