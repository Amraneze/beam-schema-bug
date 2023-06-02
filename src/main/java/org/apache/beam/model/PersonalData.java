package org.apache.beam.model;

import lombok.Builder;
import lombok.Data;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import java.io.Serializable;

@Data
@Builder
@DefaultSchema(JavaBeanSchema.class)
public class PersonalData implements Serializable {

    private static final long serialVersionUID = 1;

    private String id;
}
