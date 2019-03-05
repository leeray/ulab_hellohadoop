package com.ulabs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by leeray on 2019/3/5.
 */
public class NginxAccessLogUV {

    public static class Map01 extends Mapper<LongWritable, Text, Text, Text> {
        private Date getDateByValue(String vs) throws ParseException {
            String date = vs.substring(vs.indexOf("["), vs.indexOf("]") + 1);
            SimpleDateFormat format = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss Z]", Locale.US);
            Date d = format.parse(date);
            return d;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String vs = value.toString();
                String[] arr = vs.split("- -");
                String ip = arr[0].trim(); // IP
                String v = arr[1].trim();  // others

                if (!v.contains("/lesson/lessonFree/free/statistics")) {
                    return;
                }

                String regex = "userId=\\d?&";
                Pattern p = Pattern.compile(regex);
                Matcher matcher = p.matcher(v);

                int startIndex = 0;
                int endIndex = 0;
                while(matcher.find()) {
                    startIndex = matcher.start();
                    endIndex = matcher.end();

                    break;
                }
                if (startIndex <= 0 && endIndex <= 0) {
                    return;
                }

                String userId = "";
                userId = v.substring(startIndex, endIndex-1).split("=")[1];
                System.out.println("startIdex:"+startIndex+ "  endIndex:"+endIndex + " userId:"+userId);

                Date d = getDateByValue(vs);// DATE
                SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
                Text k = new Text(f.format(d));
                // 以日期分组
                context.write(k, new Text(ip+"--"+userId));

            } catch (Exception e) {
                System.out.println("MAPPER ++++++++++++++++++++++++++"+e.getMessage());
            }
        }
    }

    public static class Reduce01 extends Reducer<Text, Text, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String,String> m = new HashMap<String,String>();
            for(Text value : values){
                String vs = value.toString();
                m.put(vs, "");
            }
            System.out.println("RRRRRRR <><> "+m);
            context.write(key, new IntWritable(m.size()));
        }
    }

//    /**
//     * 文件名过滤
//     *
//     */
//    public static class MyPathFilter implements PathFilter, Configurable {
//        Configuration conf = null;
//        FileSystem fs = null;
//
//        @Override
//        public Configuration getConf() {
//            return this.conf;
//        }
//
//        @Override
//        public void setConf(Configuration conf) {
//            this.conf = conf;
//        }
//
//        @Override
//        public boolean accept(Path path) {
//            try {
//                fs = FileSystem.get(conf);
//                FileStatus fileStatus = fs.getFileStatus(path);
//                if (!fileStatus.isDir()) {
//                    String fileName = path.getName();
//                    if (!fileName.contains(conf.get("pathfilter.pattern"))) {
//                        return true;
//                    }
//                }
//            } catch (IOException e) {
//                System.out.println("MyPathFilter ++++++++++++++++++++++++++");
//                e.printStackTrace();
//            }
//            return false;
//        }
//    }

    public static void main(String[] args) {

        // JobConf conf = new JobConf(MaxTptr.class);
        try {
//            String str = "[28/Feb/2019:08:27:31 +0800] \"GET /lesson/lessonFree/free/statistics?type=pv&userId=0&referer=https%3A%2F%2Fwww.linkmoc.com%2Flearning%2Flesson%2FtoListLesson%3Fpage\n" +
//                    "Num%3D5%26brand_name%3D%25E5%2585%25A8%25E9%2583%25A8%26direction_name%3D%25E5%2585%25A8%25E9%2583%25A8%26lable_name%3D%25E5%2585%25A8%25E9%2583%25A8%26uptimes%3Dup_time%26type%3D0%26na\n" +
//                    "me%3D%26lesson_category%3D HTTP/2.0\"";
//            String regex = "userId=\\d?&";
//            Pattern p = Pattern.compile(regex);
//            Matcher matcher = p.matcher(str);
//
//            int startIndex = 0;
//            int endIndex = 0;
//            while(matcher.find())
//            {
//                System.out.println(matcher.group());
//                System.out.println(matcher.start()+"...."+matcher.end());
//                startIndex = matcher.start();
//                endIndex = matcher.end();
//
//                break;
//            }
//            if (startIndex <= 0 && endIndex <= 0) {
//                return;
//            }
//
//            String userId = "";
//            userId = str.substring(startIndex, endIndex-1).split("=")[1];
//
//            System.out.println("userId = " + userId);
//
//            System.out.println("str.matches:"+ str.matches(regex));
//
//            String[] arr = str.split(regex);
//            for(String s : arr)
//            {
//                System.out.println("split:"+ s);
//            }

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "nginx access log uv");
            job.setJarByClass(NginxAccessLog.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.setMapperClass(NginxAccessLogUV.Map01.class);
            job.setReducerClass(NginxAccessLogUV.Reduce01.class);

            /**
             * map 的输出如果跟 reduce 的输出不一致则必须要做此步配置，否则会按照 reduce 的输出进行默认
             */
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // 第三个参数是要过滤的文件名关键字，默认error
            String pfk = args.length > 2 ? args[2] : "error";
            job.getConfiguration().set("pathfilter.pattern", pfk);
            //FileInputFormat.setInputPathFilter(job, MyPathFilter.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
