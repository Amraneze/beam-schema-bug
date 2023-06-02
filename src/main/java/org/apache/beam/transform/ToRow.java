package org.apache.beam.transform;

import com.github.javafaker.Faker;
import org.apache.beam.model.PersonalData;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.List;
import java.util.stream.Stream;

import static org.apache.beam.sdk.schemas.Schema.toSchema;

public class ToRow extends PTransform<PCollection<Long>, PCollection<Row>> {

    private static final Schema DATA_SCHEMA = Stream.of(Schema.Field.of("id", Schema.FieldType.STRING)).collect(toSchema());

    private static final Schema SCHEMA =
            Stream.of(Schema.Field.of("id", Schema.FieldType.STRING),
                            Schema.Field.of("email", Schema.FieldType.STRING),
                            Schema.Field.nullable("firstName", Schema.FieldType.STRING),
                            Schema.Field.nullable("lastName", Schema.FieldType.STRING),
                            Schema.Field.of("data", Schema.FieldType.array(Schema.FieldType.row(DATA_SCHEMA)))
                    )
                    .collect(toSchema());

    @Override
    public PCollection<Row> expand(PCollection<Long> input) {
        return input.apply(ParDo.of(new ToRowFn())).setCoder(SchemaCoder.of(SCHEMA));
    }

    static class ToRowFn extends DoFn<Long, Row> {

        private static final long serialVersionUID = 1;

        private transient Faker faker;

        @Setup
        public void setup() {
             faker = new Faker();
        }

        @ProcessElement
        public void processElement(OutputReceiver<Row> outputReceiver) {
            var useNames = Boolean.TRUE.equals(faker.random().nextBoolean());
            var dataRow = Row.withSchema(DATA_SCHEMA).addValue(faker.internet().uuid()).build();
            var row = Row.withSchema(SCHEMA)
                    .addValue(faker.internet().uuid())
                    .addValue(faker.internet().emailAddress())
                    .addValue(useNames ? faker.name().firstName() : null)
                    .addValue(useNames ? faker.name().lastName() : null)
                    .addValue(List.of(dataRow));
            outputReceiver.output(row.build());
        }
    }
}
