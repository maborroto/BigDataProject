/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

//hadoop jar C:\Users\Manuel\Documents\NetBeansProjects\BigDataProject\dist\BigDataProject.jar C:\Users\Manuel\Documents\BigDataTest\input C:\Users\Manuel\Documents\BigDataTest\output
package bigdataproject;
 
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 *
 * @author Manuel
 */
public class BigDataProject {

    static class MapImage extends Mapper<LongWritable, Text, Text, Text> {

        private static DoubleWritable time = new DoubleWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {

                InputStream is = new ByteArrayInputStream(value.toString().getBytes());
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                Document doc = dBuilder.parse(is);

                doc.getDocumentElement().normalize();

                NodeList nList = doc.getElementsByTagName("page");

                for (int temp = 0; temp < nList.getLength(); temp++) {

                    Node nNode = nList.item(temp);

                    if (nNode.getNodeType() == Node.ELEMENT_NODE) {

                        Element eElement = (Element) nNode;

                        String id = eElement.getElementsByTagName("id").item(0).getTextContent();
                        String name = eElement.getElementsByTagName("title").item(0).getTextContent();
                        //String gender = eElement.getElementsByTagName("gender").item(0).getTextContent();

                        // System.out.println(id + "/////////////////////////////");
                        context.write(new Text(id), new Text(name));

                    }
                }
            } catch (Exception e) {
                //  LogWriter.getInstance().WriteLog(e.getMessage());
            }
        }
    }

    static class ReducerImage extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            String row = "";
//            context.write(key, new Text(row));
//            HERE WE GOT SORTED VALUES BY TIME
            for (Text value : values) {
                context.write(key, value);
            }
            // context.write(key.key, new Text(row));
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here

        try {

            Configuration conf = new Configuration();
            // conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, 2048);

            // OR alternatively you can set it this way, the name of the
            // property is
            // "mapreduce.input.fixedlengthinputformat.record.length"
            // conf.setInt("mapreduce.input.fixedlengthinputformat.record.length",
            // 2048);
            String[] arg = new GenericOptionsParser(conf, args).getRemainingArgs();

            conf.set("START_TAG_KEY", "<page>");
            conf.set("END_TAG_KEY", "</page>");

            Job job = new Job(conf, "XML Processing Processing");
            job.setJarByClass(BigDataProject.class);
            job.setMapperClass(MapImage.class);
            job.setReducerClass(ReducerImage.class);

            job.setNumReduceTasks(3);
            job.setInputFormatClass(XMLInputFormat.class);
            // job.setOutputValueClass(TextOutputFormat.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.waitForCompletion(true);

        } catch (Exception e) {
            // LogWriter.getInstance().WriteLog("Driver Error: " + e.getMessage());
            System.out.println(e.getMessage().toString());
        }

    }

}
