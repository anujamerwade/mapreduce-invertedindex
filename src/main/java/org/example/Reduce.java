package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.HashSet;
import java.util.Set;

import java.io.IOException;

public class Reduce extends Reducer<Text, Text,Text,Text> {
    @Override
    public void reduce(final Text key, final Iterable<Text> values,
                       final Context context) throws IOException, InterruptedException {

        Set<String> uniqueFiles = new HashSet<>(); // To store unique file names for each key

        StringBuilder stringBuilder = new StringBuilder();

        for (Text value : values) {
            String filename = value.toString();

            // Modify code to append a filename after the key only once.
            // If the word is encountered again, the filename won't be added to the value list again.
            // Check if the file name is already seen for this key
            if (!uniqueFiles.contains(filename)) {
                stringBuilder.append(filename).append(" | ");
                uniqueFiles.add(filename); // Add filename to the set to mark it as seen
            }
        }

        if (stringBuilder.length() > 3) {
            // Remove the last " | " from the StringBuilder
            stringBuilder.setLength(stringBuilder.length() - 3);
        }

        context.write(key, new Text(stringBuilder.toString()));
    }

}
