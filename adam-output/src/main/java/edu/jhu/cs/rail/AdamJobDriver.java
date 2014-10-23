package edu.jhu.cs.rail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bdgenomics.formats.avro.AlignmentRecord;
import parquet.avro.AvroParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class AdamJobDriver extends Configured implements Tool {

    public static String PARAM_MANIFEST_FILE = "manifest_file";
    public static String PARAM_INPUT_PATH = "input_path";
    public static String PARAM_OUTPUT_PATH = "output_path";


    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(),
                new AdamJobDriver(), args);
        System.exit(ret);
    }

    @Override
    public int run(String[] strings) throws Exception {
        return buildAndSubmit();
    }

    private int buildAndSubmit() throws IOException, ClassNotFoundException, InterruptedException {
        final Configuration conf = getConf();
        final Job job = new Job(conf);
        job.setJarByClass(getClass());

        final String manifestFile = conf.get(PARAM_MANIFEST_FILE);
        final String inputPath = conf.get(PARAM_INPUT_PATH);
        final String outputPath = conf.get(PARAM_OUTPUT_PATH);

        // Add input data
        for (final String path : inputPath.split(",")) {
            FileInputFormat.addInputPath(job, new Path(path));
        }
        job.setInputFormatClass(TextInputFormat.class);

        // set the output format
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, new Path(outputPath));
        AvroParquetOutputFormat.setSchema(job, AlignmentRecord.SCHEMA$);
        AvroParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        AvroParquetOutputFormat.setCompressOutput(job, true);
        AvroParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);

        job.setMapperClass(AdamJobMapper.class);
        job.setReducerClass(AdamJobReducer.class);
        job.setNumReduceTasks(1);

        return job.waitForCompletion(true) ? 0 : 1;

    }
}
