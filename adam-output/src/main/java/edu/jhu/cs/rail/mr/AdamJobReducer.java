package edu.jhu.cs.rail.mr;

import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bdgenomics.formats.avro.AlignmentRecord;

import java.io.IOException;

/**
 * Reducer class.
 * This is a identity reducer which is kind of doing nothing beside writing records to correct partition.
 */
public class AdamJobReducer extends Reducer<LongWritable, AvroWrapper<AlignmentRecord>, AvroWrapper<AlignmentRecord>, NullWritable> {

    private final NullWritable nullValue = NullWritable.get();

    @Override
    protected void reduce(final LongWritable index, final Iterable<AvroWrapper<AlignmentRecord>> values, final Context context) throws IOException, InterruptedException {
        for (final AvroWrapper<AlignmentRecord> value : values) {
            context.write(value, nullValue);
        }
    }
}
