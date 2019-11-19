package com.claus.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MetricEvent {

    private String name;
    private Long timestamp;
    // Metric fields
    private Map<String, Object> fields;
    // Metric tags
    private Map<String, String> tags;
}
