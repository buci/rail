package edu.jhu.cs.rail.mr;

import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.bdgenomics.formats.avro.AlignmentRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * This partition class is responsible for assignment of a alignment-start to reducer number.
 * Our aim is to assign each reducer non-overlapping (partial overlapping) alignment range wrt reference genome.
 */
public class AdamLogPartitioner extends Partitioner<LongWritable, AvroWrapper<AlignmentRecord>> {

    private final Map<Long, Integer> partitionMap = new HashMap<Long, Integer>();

    public AdamLogPartitioner() {
        //TODO we can load a configuration file here. Or just do a uniform partition.
    }

    @Override
    public int getPartition(final LongWritable key, final AvroWrapper<AlignmentRecord> value, final int i) {
        return 0; //TODO fix this.
    }
}
