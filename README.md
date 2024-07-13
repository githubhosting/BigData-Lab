## Commands to run the code

```bash
javac -d . *.java
echo Main-Class: oddeven.driver > Manifest.txt
jar -cfm oddeven.jar Manifest.txt oddeven/*.class
hadoop jar weather.jar oe.txt output
cat output/*
```

## 1. Word Count

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

## 2. Odd Even

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

### 3. Weather Report

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

## 4. Insurance Data

```java
<!-- driver.java -->

package insurance;
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

package insurance;
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
    String[] line = value.toString().split(",");
    output.collect(new Text(line[2]), new IntWritable(1));
  }
}

<!-- reducer.java -->

package insurance;
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
    int sum = 0;
    while (value.hasNext()) {
      sum += value.next().get();
    }
    output.collect(key, new IntWritable(sum));
  }
}
```

## 5. Sales Records

```java
<!-- driver.java -->

package sales;
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

package sales;
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
    String[] line = value.toString().split(",");
    int price = Integer.parseInt(line[2]);
    String cardtype = line[3];
    String Country = line[7];
    output.collect(new Text("Country " + Country), new IntWritable(price));
    output.collect(new Text("CardType " + cardtype), new IntWritable(1));
  }
}

<!-- reducer.java -->

package sales;
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
    int sum = 0;
    while (value.hasNext()) {
      sum += value.next().get();
    }
    output.collect(new Text(key), new IntWritable(sum));
  }
}
```

## 6. Employee Records

```java
<!-- driver.java -->

package employee;
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

package employee;
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

class mapper
  extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, DoubleWritable> {
  public void map(
    LongWritable key,
    Text value,
    OutputCollector<Text, DoubleWritable> output,
    Reporter r
  ) throws IOException {
    String[] line = value.toString().split("\\t");
    double salary = Double.parseDouble(line[8]);
    output.collect(new Text(line[3]), new DoubleWritable(salary));
  }
}

<!-- reducer.java -->

package employee;
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
    int count = 0;
    double sum = 0.0;
    while (value.hasNext()) {
      sum += value.next().get();
      count += 1;
    }
    output.collect(new Text(key + " Average"), new DoubleWritable(sum / count));
    output.collect(new Text(key + " Count"), new DoubleWritable(count));
  }
}
```

## 7. Matrix Multiplication

```java
<!-- driver.java -->

package matrix;
import java.util.*;
import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

public class driver {
    public static void main(String args[]) throws IOException {
        JobConf conf = new JobConf(driver.class);
        conf.setMapperClass(mapper.class);
        conf.setReducerClass(reducer.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}

<!-- mapper.java -->

package matrix;
import java.util.*;
import java.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

class mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter r) throws IOException {
        String line[] = value.toString().split(",");
        Text OutputKey = new Text();
        Text OutputValue = new Text();
        if (line[0].equals("A")) {
            for (int i = 0; i < 3; i++) {
                OutputKey.set(line[1] + "," + i);
                OutputValue.set("A," + line[2] + "," + line[3]);
                output.collect(OutputKey, OutputValue);
            }
        } else {
            for (int i = 0; i < 2; i++) {
                OutputKey.set(i + "," + line[2]);
                OutputValue.set("B," + line[1] + "," + line[3]);
                output.collect(OutputKey, OutputValue);
            }
        }
    }
}

<!-- reducer.java -->

package matrix;
import java.util.*;
import java.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

public class reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> output, Reporter r)
            throws IOException {
        HashMap<Integer, Float> a = new HashMap<Integer, Float>();
        HashMap<Integer, Float> b = new HashMap<Integer, Float>();
        String[] v;
        while (value.hasNext()) {
            v = value.next().toString().split(",");
            if (v[0].equals("A")) {
                a.put(Integer.parseInt(v[1]), Float.parseFloat(v[2]));
            } else {
                b.put(Integer.parseInt(v[1]), Float.parseFloat(v[2]));
            }
        }
        float aij, bij, result = 0.0f;
        for (int i = 0; i < 5; i++) {
            aij = a.containsKey(i) ? a.get(i) : 0.0f;
            bij = b.containsKey(i) ? b.get(i) : 0.0f;
            result += aij * bij;
        }
        if (result != 0.0f) {
            output.collect(null, new Text(key + "," + Float.toString(result)));
        }
    }
}
```

## Commands to run spark code

```bash
cd spark-3.5.1-bin-hadoop3
source bash.sh
spark-shell
```

then press `ctrl + c` to exit the shell

```bash
spark-submit file.py input.txt output
cat output/*
```

## Commands to run the Pig code

```bash
pig -x local file.pig
```

## 11. FILTER and GROUP Students details

```pig
student_details = LOAD 'student.txt' USING
PigStorage(',') as (id:int, firstname:chararray, lastname:chararray, age:int, phone:chararray, city:chararray);

filter_data = FILTER student_details by city == 'Chennai';
STORE filter_data INTO 'filter';
group_data = GROUP student_details by age;
STORE group_data INTO 'group';
```

## 12. JOIN and SORT Customer and Order details

```pig
customers = LOAD 'customer.txt' USING
PigStorage(',') as (id:int, name:chararray, age:int, address:chararray, salary:int);

orders = LOAD 'order.txt' USING
PigStorage(',') as (oid:int, date:chararray, customer_id:int, amount:int);

join_result = JOIN customers BY id, orders BY customer_id;
STORE join_result INTO 'joinoutput';
sorting = ORDER join_result by age ASC;
STORE sorting into 'sortoutput';
```
