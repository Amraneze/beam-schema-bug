package org.apache.beam.model;

import lombok.Data;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;

@Data
@DefaultSchema(JavaBeanSchema.class)
public class Message implements Serializable {

    private static final long serialVersionUID = 1;

    private String id;
    private String email;
    @Nullable private String firstName;
    @Nullable private String lastName;
    private List<PersonalData> data;

}
