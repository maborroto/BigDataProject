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
import java.nio.charset.Charset;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

    static class MapImage extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                InputStream inputS = new ByteArrayInputStream(value.toString().getBytes(Charset.forName("UTF-8")));
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder dBuilder = factory.newDocumentBuilder();
                Document document = dBuilder.parse(inputS);
                document.getDocumentElement().normalize();
                NodeList nodes = document.getElementsByTagName("page");
                for (int i = 0; i < nodes.getLength(); i++) {

                    Node node = nodes.item(i);
                    if (node.getNodeType() == Node.ELEMENT_NODE) {
                        Element eElement = (Element) node;
                        String id = eElement.getElementsByTagName("id").item(0).getTextContent();
                        String text = eElement.getElementsByTagName("text").item(0).getTextContent();
                        //String gender = eElement.getElementsByTagName("gender").item(0).getTextContent();

                        String imgProp = extractImage(text);
                        if (!imgProp.isEmpty()) {
                            context.write(new LongWritable(Long.valueOf(id)), new Text(imgProp));
                        }

                    }
                }
            } catch (Exception e) {
                //  LogWriter.getInstance().WriteLog(e.getMessage());
                e.printStackTrace();
            }
        }

        private static String extractImage(String textTag) {
            //"\\[[fF]ile\\s*:\\s*.*\\.JPG|jpg|JPEG|jpeg|GIF|gif|PNG|png|tiff|TIFF|BMP|bmp:";
            //".*\\|\\s*[Ii]mmagine\\s*=\\s*.*\\.JPG|jpg|JPEG|jpeg|GIF|gif|PNG|png|tiff|TIFF|BMP|bmp:";
            String regExp1 = "[fF]ile\\s*:\\s*.*?\\.(JPG|jpg|JPEG|jpeg|GIF|gif|PNG|png|tiff|TIFF|BMP|bmp|SVG|svg)";
            String regExp2 = "[Ii]mmagine\\s*=\\s*.*?\\.(JPG|jpg|JPEG|jpeg|GIF|gif|PNG|png|tiff|TIFF|BMP|bmp|SVG|svg)";
            String text = textTag.replaceAll("(\\|)|(\\[|\\])|(\\{|\\})", "\n");
//            text = text.replaceAll("<!--.*?-->", "");
            Pattern pattern1 = Pattern.compile(regExp1);
            Pattern pattern2 = Pattern.compile(regExp2);
            Matcher matcher = pattern1.matcher(text);

            int pos1 = Integer.MAX_VALUE;
            int end1 = Integer.MAX_VALUE;
            int pos2 = Integer.MAX_VALUE;
            int end2 = Integer.MAX_VALUE;
            if (matcher.find()) {
                pos1 = matcher.start();
                end1 = matcher.end();
            }

            matcher = pattern2.matcher(text);
            if (matcher.find()) {
                pos2 = matcher.start();
                end2 = matcher.end();
            }

            if (pos1 != Integer.MAX_VALUE || pos2 != Integer.MAX_VALUE) {
                if (pos1 < pos2) {
                    String img = textTag.substring(pos1, end1);
                    img = img.replaceAll(".*[fF]ile\\s*:\\s*", "");
                    img = img.replaceAll("<!--.*?(JPG|jpg|JPEG|jpeg|GIF|gif|PNG|png|tiff|TIFF|BMP|bmp)", "");
                    return img.trim();
                } else if (pos2 < pos1) {
                    String img = textTag.substring(pos2, end2);
                    img = img.replaceAll(".*[Ii]mmagine\\s*=\\s*", "");
                    img = img.replaceAll(".*[fF]ile\\s*:\\s*", "");
                    img = img.replaceAll("<!--.*?(JPG|jpg|JPEG|jpeg|GIF|gif|PNG|png|tiff|TIFF|BMP|bmp)", "");
                    return img.trim();
                }
            }
            return "";
        }
    }

    static class ReducerImage extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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

            Job job = Job.getInstance(conf, "Image Procesing");
            job.setJarByClass(BigDataProject.class);
            job.setMapperClass(MapImage.class);
            job.setReducerClass(ReducerImage.class);

//            job.setNumReduceTasks(3);
            job.setInputFormatClass(XMLInputFormat.class);
            // job.setOutputValueClass(TextOutputFormat.class);

            job.setOutputKeyClass(LongWritable.class);
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
