package org.operaton.bpm.extension.nifi.processor;

import org.apache.nifi.components.DescribedValue;

/**
 * How process variables are represented in FlowFile content.
 */
public enum VariablesFormat implements DescribedValue {

    SIMPLE("Simple JSON",
            "A JSON object of variable names and plain values. Objects and arrays are "
                    + "exchanged as Operaton Json variables, which requires the Spin JSON plugin."),
    TYPED("Operaton Typed JSON",
            "A JSON object of variable names and Operaton typed values, "
                    + "e.g. {\"name\": {\"type\": \"String\", \"value\": \"x\"}}."),
    NONE("None",
            "FlowFile content is not used for variables.");

    private final String displayName;
    private final String description;

    VariablesFormat(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
