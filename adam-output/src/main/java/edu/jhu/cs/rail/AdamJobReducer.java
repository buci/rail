package edu.jhu.cs.rail;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bdgenomics.formats.avro.AlignmentRecord;

import java.io.IOException;

public class AdamJobReducer extends Reducer<Text, BytesWritable, AvroWrapper<AlignmentRecord>, NullWritable> {

    private final NullWritable nullValue = NullWritable.get();
    private SpecificDatumReader<AlignmentRecord> reader = new SpecificDatumReader<AlignmentRecord>(AlignmentRecord.getClassSchema());

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        for (BytesWritable bytesWritable : values) {
            Decoder decoder = DecoderFactory.get().binaryDecoder(bytesWritable.getBytes(), null);
            AlignmentRecord alignmentRecord = reader.read(null, decoder);
            context.write(new AvroWrapper<AlignmentRecord>(alignmentRecord), nullValue);
        }
    }
}
