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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogWriter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

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

                        String imgProp = extractImage(text);
                        if (!imgProp.isEmpty()) {
                            imgProp = imgProp.replaceAll(" ", "_");
                            imgProp = ("\"") + imgProp + ("\"");
                            context.write(new LongWritable(Long.valueOf(id)), new Text(imgProp));
                        }

                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * Extract the the first image from the WIKI Text Tag
         *
         * @param textTag
         * @return the image name
         */
        private static String extractImage(String textTag) {
            //REGULAR EXPRESION TO DETECT THE IMAGES IN THE TEXT
            String regExp1 = "([fF]ile\\s*:\\s*.*?\\.(JPG|jpg|JPEG|jpeg|GIF|gif|PNG|png|tiff|TIFF|BMP|bmp|SVG|svg|XCF|xcf))|"
                    + "([Ii]mmagine\\s*[=:]\\s*.*?\\.(JPG|jpg|JPEG|jpeg|GIF|gif|PNG|png|tiff|TIFF|BMP|bmp|SVG|svg|XCF|xcf))|"
                    + "([Ii]mage\\s*[=:]\\s*.*?\\.(JPG|jpg|JPEG|jpeg|GIF|gif|PNG|png|tiff|TIFF|BMP|bmp|SVG|svg|XCF|xcf))|"
                    + "([Bb]andiera\\s*[=:]\\s*.*?\\.(JPG|jpg|JPEG|jpeg|GIF|gif|PNG|png|tiff|TIFF|BMP|bmp|SVG|svg|XCF|xcf))|"
                    + "([Pp]anorama\\s*[=:]\\s*.*?\\.(JPG|jpg|JPEG|jpeg|GIF|gif|PNG|png|tiff|TIFF|BMP|bmp|SVG|svg|XCF|xcf))";
            String text = textTag.replaceAll("(\\|)|(\\[|\\])|(\\{|\\})", "\n");
            Pattern pattern1 = Pattern.compile(regExp1);
            Matcher matcher = pattern1.matcher(text);

            int pos = Integer.MAX_VALUE;
            int end = Integer.MAX_VALUE;

            //FINDING THE FIRST MATCHING
            if (matcher.find()) {
                pos = matcher.start();
                end = matcher.end();
            }

            if (pos != Integer.MAX_VALUE /*|| pos2 != Integer.MAX_VALUE*/) {
                String img = textTag.substring(pos, end);
                //REMOVING THE IMAGE TAG TO THE IMAGE NAME AND REMOVING THE COMMENTS
                img = img.replaceAll("(.*[fF]ile\\s*:\\s*)|([Ii]mmagine\\s*[=:]\\s*)|([Ii]mage\\s*[=:]\\s*)|([Bb]andiera\\s*[=:]\\s*)", "");
                img = img.replaceAll("<!--.*?(JPG|jpg|JPEG|jpeg|GIF|gif|PNG|png|tiff|TIFF|BMP|bmp|SVG|svg|XCF|xcf)", "");
                img = img.replaceAll("\"", "\"\"");
                return img.trim();
            }
            return "";
        }
    }

    static class ReducerImage extends Reducer<LongWritable, Text, Text, NullWritable> {

        private static boolean exReducer = false;
        private final String HEADER = "pp_page;pp_value";

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text value : values) {
                String row = "";
                if (!exReducer) {
                    row = HEADER + "\n";
                }
                row += (key + ";" + value);
                context.write(new Text(row), NullWritable.get());
            }
            if (!exReducer) {
                exReducer = !exReducer;
            }
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here

        try {
            Configuration conf = new Configuration();
            String[] arg = new GenericOptionsParser(conf, args).getRemainingArgs();

            conf.set("START_TAG_KEY", "<page>");
            conf.set("END_TAG_KEY", "</page>");

            Job job = Job.getInstance(conf, "Image Procesing");
            job.setJarByClass(BigDataProject.class);
            job.setMapperClass(MapImage.class);
            job.setReducerClass(ReducerImage.class);
            job.setInputFormatClass(XMLInputFormat.class);
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
