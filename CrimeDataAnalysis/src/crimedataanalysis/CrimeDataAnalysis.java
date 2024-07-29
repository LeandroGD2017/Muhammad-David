/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package crimedataanalysis;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class CrimeDataAnalysis {

    public static class CrimeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        // value to represent count of one.  
        // crime type.
        private final static IntWritable one = new IntWritable(1);
        private Text crimeType = new Text();

        // map that processes each line of the input file
        public void map(LongWritable key, Text value, Context context) {
            String[] fields = value.toString().split(",");
            if (fields.length > 5) {
                String primaryType = fields[5];
                word.set(primaryType);
                context.write(word, one);
            }

        }
    }
    
    public static class CrimeReducer extends Reducer <Text, IntWritable, Text, IntWritable> {
        
    } 
    

    public static void main(String[] args) {

    }

}
