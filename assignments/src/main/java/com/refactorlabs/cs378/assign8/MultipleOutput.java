package com.refactorlabs.cs378.assign8;

import com.refactorlabs.cs378.assign7.VinImpressionCounts;
import com.refactorlabs.cs378.sessions.Event;
import com.refactorlabs.cs378.sessions.EventSubtype;
import com.refactorlabs.cs378.sessions.EventType;
import com.refactorlabs.cs378.sessions.Session;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.commons.collections.map.DefaultedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
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
public class MultipleOutput extends Configured implements Tool {

    /**
     * The Map class for word statistics.  Extends class Mapper, provided by Hadoop.
     * This class defines the map() function for the word statistics example.
     */

    public static enum SESSION_COUNTERS {
        SUBMITTER,
        SHARER,
        CLICKER,
        SHOWER,
        VISITOR,
        OTHER
    };

    public static class SessionMapClass extends Mapper<AvroKey<Session>, NullWritable, AvroKey<CharSequence>, AvroValue<Session>> {

        private AvroMultipleOutputs multipleOutputs;

        public void setup(Context context) {
            multipleOutputs = new AvroMultipleOutputs(context);
        }

        @Override
        public void map(AvroKey<Session> value, NullWritable _, Context context)
                throws IOException, InterruptedException {
            CharSequence key = value.datum().getUserId();
            List<Event> events = value.datum().getEvents();
            Set<EventType> set = new HashSet<EventType>();

            if(events.size() > 1000) return;

            for (Event event: events) {
                set.add(event.getEventType());
            }

            String sessionType;

            if(set.contains(EventType.CHANGE) || set.contains(EventType.CONTACT_FORM_STATUS) ||
                    set.contains(EventType.EDIT) || set.contains(EventType.SUBMIT)) {
                sessionType = "Submitter";
                context.getCounter(SESSION_COUNTERS.SUBMITTER).increment(1);
            } else if(set.contains(EventType.SHARE)) {
                sessionType = "Sharer";
                context.getCounter(SESSION_COUNTERS.SHARER).increment(1);
            } else if(set.contains(EventType.CLICK)) {
                sessionType = "Clicker";
                context.getCounter(SESSION_COUNTERS.CLICKER).increment(1);
            } else if(set.contains(EventType.SHOW)) {
                sessionType = "Shower";
                context.getCounter(SESSION_COUNTERS.SHOWER).increment(1);
            } else if(set.size() == 1 && set.contains(EventType.VISIT)) {
                sessionType = "Visitor";
                context.getCounter(SESSION_COUNTERS.VISITOR).increment(1);
            } else {
                sessionType = "Other";
                context.getCounter(SESSION_COUNTERS.OTHER).increment(1);
            }

            multipleOutputs.write (sessionType,
                                   new AvroKey<CharSequence>(key.toString()),
                                   new AvroValue<Session>(value.datum()),
                                   sessionType);

        }

        public void cleanup(Context context) throws InterruptedException , IOException {
            multipleOutputs.close();
        }

    }


    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MultipleOutput <input path1> <input path2> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = new Job(conf, "MultipleOutput");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(MultipleOutput.class);
        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.set("mapreduce.user.classpath.first", "true");

        // Specify the Map
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setMapperClass(MapClass.class);
//        job.setMapOutputKeyClass(Text.class);

//        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
//        AvroJob.setMapOutputValueSchema(job, VinImpressionCounts.getClassSchema());
        AvroJob.setInputKeySchema(job, Session.getClassSchema());
//        MultipleInputs.addInputPath(job, new Path(appArgs[0]), AvroKeyInputFormat.class, SessionMapClass.class);

        MultipleOutputs.addNamedOutput(job, "sessionType", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.setCountersEnabled(job, true);
        AvroMultipleOutputs.addNamedOutput(job, "sessionType", AvroKeyValueOutputFormat.class ,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());

        //Specify the Combiner

//        job.setCombinerClass(CombinerClass.class);

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setReducerClass(ReduceClass.class);
//        AvroJob.setOutputKeySchema(job,
//                Pair.getPairSchema(Schema.create(Schema.Type.STRING), VinImpressionCounts.getClassSchema()));
        job.setOutputValueClass(NullWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(appArgs[2]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

        Counters counters = job.getCounters();
        System.out.println(counters.findCounter(SESSION_COUNTERS.SUBMITTER).getDisplayName() + ":" +
                counters.findCounter(SESSION_COUNTERS.SUBMITTER).getValue());
        return 0;
    }
    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        // printClassPath();
        int res = ToolRunner.run(new Configuration(), new MultipleOutput(), args);
        System.exit(res);
    }

}
