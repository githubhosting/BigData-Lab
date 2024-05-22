## Odd Even

```java
<!-- driver.java -->

package oddeven;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class driver {

  public static void main(String args[]) throws IOException {
    JobConf conf = new JobConf(driver.class);
    conf.setMapperClass(mapper.class);
    conf.setReducerClass(reducer.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    JobClient.runJob(conf);
  }
}

<!-- mapper.java -->

package oddeven;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper
  extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {

  public void map(
    LongWritable key,
    Text value,
    OutputCollector<Text, IntWritable> output,
    Reporter r
  ) throws IOException {
    String[] line = value.toString().split(" ");
    for (String num : line) {
      int number = Integer.parseInt(num);
      if (number % 2 == 0) {
        output.collect(new Text("even"), new IntWritable(number));
      } else {
        output.collect(new Text("odd"), new IntWritable(number));
      }
    }
  }
}

<!-- reducer.java -->

package oddeven;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class reducer
  extends MapReduceBase
  implements Reducer<Text, IntWritable, Text, IntWritable> {

  public void reduce(
    Text key,
    Iterator<IntWritable> value,
    OutputCollector<Text, IntWritable> output,
    Reporter r
  ) throws IOException {
    int sum = 0, count = 0;
    while (value.hasNext()) {
      sum += value.next().get();
      count++;
    }
    output.collect(
      new Text("Sum of " + key + " Numbers"),
      new IntWritable(sum)
    );
    output.collect(new Text(key + " Number count"), new IntWritable(count));
  }
}

```

### Word Count

```java
<!-- driver.java -->

package wordcount;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class driver {

  public static void main(String args[]) throws Exception {
    JobConf conf = new JobConf(driver.class);
    conf.setMapperClass(mapper.class);
    conf.setReducerClass(reducer.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    JobClient.runJob(conf);
  }
}

<!-- mapper.java -->

package wordcount;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper
  extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {

  public void map(
    LongWritable key,
    Text value,
    OutputCollector<Text, IntWritable> output,
    Reporter r
  ) throws IOException {
    String line[] = value.toString().split(" ");
    for (String a : line) {
      output.collect(new Text(a), new IntWritable(1));
    }
  }
}

<!-- reducer.java -->

package wordcount;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

class reducer
  extends MapReduceBase
  implements Reducer<Text, IntWritable, Text, IntWritable> {

  public void reduce(
    Text key,
    Iterator<IntWritable> value,
    OutputCollector<Text, IntWritable> output,
    Reporter r
  ) throws IOException {
    int count = 0;
    while (value.hasNext()) {
      count += value.next().get();
    }
    output.collect(new Text(key), new IntWritable(count));
  }
}

```

### Weather Report

```java
<!-- driver.java -->

package weather;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class driver {

  public static void main(String args[]) throws IOException {
    JobConf conf = new JobConf(driver.class);
    conf.setMapperClass(mapper.class);
    conf.setReducerClass(reducer.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    JobClient.runJob(conf);
  }
}

<!-- mapper.java -->

package weather;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper
  extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, DoubleWritable> {

  public void map(
    LongWritable key,
    Text value,
    OutputCollector<Text, DoubleWritable> output,
    Reporter r
  ) throws IOException {
    String line = value.toString();
    String year = line.substring(15, 19);
    Double temp = Double.parseDouble(line.substring(87, 92));
    output.collect(new Text(year), new DoubleWritable(temp));
  }
}

<!-- reducer.java -->

package weather;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

class reducer
  extends MapReduceBase
  implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

  public void reduce(
    Text key,
    Iterator<DoubleWritable> value,
    OutputCollector<Text, DoubleWritable> output,
    Reporter r
  ) throws IOException {
    Double max = -9999.0;
    Double min = 9999.0;
    while (value.hasNext()) {
      Double temp = value.next().get();
      max = Math.max(max, temp);
      min = Math.min(min, temp);
    }
    output.collect(new Text("Max temp at " + key), new DoubleWritable(max));
    output.collect(new Text("Min temp at " + key), new DoubleWritable(min));
  }
}

```