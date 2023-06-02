package org.apache.beam.transform;

import org.apache.beam.model.Message;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

public class ToMessageConverterFn extends DoFn<Row, Message> {

    private static final long serialVersionUID = 1;

    @ProcessElement
    public void processElement(@Element Row row, OutputReceiver<Message> outputReceiver) throws NoSuchSchemaException {
        var message = SchemaRegistry.createDefault().getFromRowFunction(Message.class).apply(row);
        outputReceiver.output(message);
    }
}
