package com.pipeline.streaming.processor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;

/* serialversionuid must stay pinned — flink checkpoints serialize this class directly,
 * so a change without bumping the uid will fail to restore from any existing checkpoint */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class PageviewEvent implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @JsonProperty("user_id")
    private long userId;

    @JsonProperty("postcode")
    private String postcode;

    @JsonProperty("webpage")
    private String webpage;

    @JsonProperty("timestamp")
    private long timestamp;
}
