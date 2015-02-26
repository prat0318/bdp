package com.refactorlabs.cs378.wordcount;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Map class for various WordCount examples that use the AVRO generated class WordCountData.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, AvroValue<WordCountData>> {

    /**
     * Local variable "word" will contain the word identified in the input.
     */
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);

        // For each word in the input line, emit a count of 1 for that word.
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());

            WordCountData.Builder builder = WordCountData.newBuilder();
            builder.setCount(Utils.ONE);
            context.write(word, new AvroValue(builder.build()));
            context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Words").increment(1L);
        }
    }
}
