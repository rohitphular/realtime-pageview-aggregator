package com.pipeline.streaming.producer.model;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
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