package edu.jhu.cs.rail;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMLineParser;
import htsjdk.samtools.SAMRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bdgenomics.adam.converters.SAMRecordConverter;
import org.bdgenomics.adam.models.RecordGroupDictionary;
import org.bdgenomics.adam.models.SequenceDictionary;
import org.bdgenomics.adam.serialization.AvroSerializer;
import org.bdgenomics.formats.avro.AlignmentRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by ankit on 10/22/14.
 */
public class AdamJobMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
    private SAMLineParser samLineParser;
    private SequenceDictionary seqDict;
    private RecordGroupDictionary rgDict;
    private SAMRecordConverter samRecordConverter;

    private Text mapperEmitKey = new Text();
    private BytesWritable mapperEmitValue = new BytesWritable();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    DatumWriter<AlignmentRecord> writer = new SpecificDatumWriter<AlignmentRecord>(AlignmentRecord.getClassSchema());

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        final SAMFileHeader samFileHeader = new SAMFileHeader();// TODO need to read this properly.
        samLineParser = new SAMLineParser(samFileHeader);
        seqDict = SequenceDictionary.fromSAMHeader(samFileHeader);
        rgDict = RecordGroupDictionary.fromSAMHeader(samFileHeader);
        samRecordConverter = new SAMRecordConverter();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final SAMRecord samRecord = samLineParser.parseLine(value.toString());
        final AlignmentRecord convert = samRecordConverter.convert(samRecord, seqDict, rgDict);
        mapperEmitKey.set(String.valueOf(convert.getStart()));
        writer.write(convert, encoder);
        encoder.flush();
        out.close();
        byte[] serializedBytes = out.toByteArray();
        mapperEmitValue.set(serializedBytes, 0, serializedBytes.length);
        context.write(mapperEmitKey, mapperEmitValue);
    }
}
