package org.apache.beam.transform;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.model.Message;
import org.apache.beam.sdk.transforms.DoFn;

@Slf4j
@AllArgsConstructor
public class LogFn extends DoFn<Message, Void> {

    private final String step;

    @ProcessElement
    public void processElement(@Element Message message) {
        log.info("Message Received for step {}: {}", step, message);
    }
}