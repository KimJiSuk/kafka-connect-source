package com.kafka.connect.templating;

import java.util.*;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Template {
    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\{\\{\\s*([\\w_]+)\\s*}}"); // {{ var }}

    private final String originalTemplateString;

    private final List<String> variables = new ArrayList<>();
    private final List<TemplatePart> templateParts = new ArrayList<>();

    public Template(final String template) {
        this.originalTemplateString = template;

        final Matcher m = VARIABLE_PATTERN.matcher(template);
        int position = 0;
        while (m.find()) {
            templateParts.add(new StaticTemplatePart(template.substring(position, m.start())));

            final String variableName = m.group(1);
            variables.add(variableName);
            templateParts.add(new DynamicTemplatePart(variableName, m.group()));
            position = m.end();
        }
        templateParts.add(new StaticTemplatePart(template.substring(position)));
    }

    public final List<String> variables() {
        return Collections.unmodifiableList(variables);
    }

    public final Set<String> variablesSet() {
        return Collections.unmodifiableSet(new HashSet<>(variables));
    }

    public final Instance instance() {
        return new Instance();
    }

    private abstract static class TemplatePart {
    }

    private static final class StaticTemplatePart extends TemplatePart {
        final String text;

        StaticTemplatePart(final String text) {
            this.text = text;
        }
    }

    private static final class DynamicTemplatePart extends TemplatePart {
        final String variableName;
        final String originalPlaceholder;

        DynamicTemplatePart(final String variableName, final String originalPlaceholder) {
            this.variableName = variableName;
            this.originalPlaceholder = originalPlaceholder;
        }
    }

    @Override
    public String toString() {
        return originalTemplateString;
    }

    public class Instance {
        private final Map<String, Supplier<String>> bindings = new HashMap<>();

        private Instance() {
        }

        public final Instance bindVariable(final String name, final Supplier<String> supplier) {
            Objects.requireNonNull(name);
            Objects.requireNonNull(supplier);
            if (name.trim().isEmpty()) {
                throw new IllegalArgumentException("name must not be empty");
            }
            bindings.put(name, supplier);
            return this;
        }

        public final String render() {
            final StringBuilder sb = new StringBuilder();
            for (final TemplatePart templatePart : templateParts) {
                if (templatePart instanceof StaticTemplatePart) {
                    sb.append(((StaticTemplatePart) templatePart).text);
                } else if (templatePart instanceof DynamicTemplatePart) {
                    final DynamicTemplatePart dynamicTemplatePart = (DynamicTemplatePart) templatePart;
                    final Supplier<String> supplier = bindings.get(dynamicTemplatePart.variableName);
                    // Substitute for bound variables, pass the variable pattern as is for non-bound.
                    if (supplier != null) {
                        sb.append(supplier.get());
                    } else {
                        sb.append(dynamicTemplatePart.originalPlaceholder);
                    }
                }
            }
            return sb.toString();
        }
    }
}
