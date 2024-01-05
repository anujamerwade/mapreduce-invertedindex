package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;
import java.text.Normalizer;

public class Map extends Mapper<LongWritable,Text,Text,Text> {

    private Text word = new Text();
    private Text filename = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        FileSplit currentSplit = ((FileSplit) context.getInputSplit());
        String filenameStr = currentSplit.getPath().getName();
        filename = new Text(filenameStr);

        String line = value.toString();

        // case normalization
        String normalizedLine = Normalizer.normalize(line, Normalizer.Form.NFD).toLowerCase();

        // remove punctuation
        String newLine = normalizedLine.replaceAll("[\\p{Punct}]", "");

        // line tokenization
        StringTokenizer tokenizer = new StringTokenizer(newLine);

        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();

            // Check if the token contains at least one alphabetical character
            if (token.matches(".*[a-zA-Z].*")) {
                // Ensure the token is composed solely of letters
                if (token.matches("[a-zA-Z]+")) {
                    word.set(token);
                    context.write(word, filename);
                }
            }
        }

    }
}