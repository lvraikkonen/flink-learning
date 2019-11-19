package com.claus.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class LogEvent {

    // the type of the log (app, docker, ...etc.)
    private String type;
    private Long timestamp;
    // the level of log (DEBUG, INFO, WARN, ERROR)
    private String level;
    private String message;
    private Map<String, String> tags = new HashMap<>();
}
