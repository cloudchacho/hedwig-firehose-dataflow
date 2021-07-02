package io.github.cloudchacho;

import io.github.cloudchacho.hedwig.Container;
import io.github.cloudchacho.hedwig.Options;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * This pipeline ingests incoming data from a Cloud Pub/Sub topic and
 * outputs the proto JSON data into windowed files at the specified output
 * directory.
 *
 * <b>Example Usage:</b>
 *
 * <pre>
 * mvn compile exec:java \
 -Dexec.mainClass=[MAIN CLASS] \
 -Dexec.cleanupDaemonThreads=false \
 -Dexec.args=" \
 --project=${PROJECT_ID} \
 --stagingLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/staging \
 --tempLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/temp \
 --runner=DataflowRunner \
 --windowDuration=2m \
 --numShards=1 \
 --inputSubscriptions=hedwig-firehose-${TOPIC_ID} \
 --inputSubscriptionsCrossProject=hedwig-firehose-${PROJECT_ID}-${TOPIC_ID};${PROJECT_ID} \
 --userTempLocation=gs://${PROJECT_ID}/tmp/ \
 --outputDirectory=gs://${PROJECT_ID}/output/ \
 --region=${REGION} \
 --zone=${ZONE} \
 --workerLogLevelOverrides='{\"io.github.cloudchacho.Firehose\":\"DEBUG\"}'"
 * </pre>
 */
public class Firehose {

    private static class DatePartitionedName implements FileIO.Write.FileNaming {
        private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern("yyyy/MM/dd");
        private static final DateTimeFormatter TIME_FORMAT = DateTimeFormat.forPattern("HH:mm:ss");

        // ProtobufDecoder sets group id as `<project_id>/<message_type>`
        private static final Pattern FILENAME_PATTERN = Pattern.compile("([^/]+)/(.+)");

        private final String projectId;
        private final String topic;

        DatePartitionedName(String name) {
            Matcher matcher = FILENAME_PATTERN.matcher(name);
            if (!matcher.matches()) {
                throw new RuntimeException(String.format(
                    "filename: %s doesn't match pattern: %s", name, FILENAME_PATTERN.toString()));
            }
            projectId = matcher.group(1);
            topic = matcher.group(2);
        }

        @Override
        public String getFilename(
            BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
            IntervalWindow intervalWindow = (IntervalWindow) window;

            return String.format(
                "%s/%s/%s/%s-%s-%s-%s-of-%s%s",
                projectId,
                topic,
                DATE_FORMAT.print(intervalWindow.start()),
                topic,
                TIME_FORMAT.print(intervalWindow.start()),
                TIME_FORMAT.print(intervalWindow.end()),
                shardIndex,
                numShards,
                compression.getSuggestedSuffix());
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(Firehose.class);

    // reverse of https://github.com/google/gson/blob/3958b1f78dc2b12da3ffd7426d3fc90550d46758/gson/src/main/java/com/google/gson/stream/JsonWriter.java#L157
    private static final Map<String, String> HTML_SAFE_REPLACEMENT_CHARS = Map.of(
        "\\\\u003c", "<",
        "\\\\u003e", ">",
        "\\\\u0026", "&",
        "\\\\u003d", "=",
        "\\\\u0027", "\""
    );

    private static final String unknownMessageGroupId = "unknown";

    private static final Pattern schemaPattern = Pattern.compile("^(.*)/(\\d*)\\.(\\d*)$");

    // name be like
    // hedwig-firehose-user-created-v1
    private static final Pattern SUBSCRIPTION_REGEXP = Pattern.compile("hedwig-firehose-(.+)");

    // name be like
    // hedwig-firehose-other-project-user-created-v1;other-project
    private static final Pattern SUBSCRIPTION_CROSS_PROJECT_REGEXP = Pattern.compile("hedwig-firehose-(.+)-(.+);\\1");

    private static final Map<Integer, Message> emptyMap = new HashMap<>();

    private static final Base64.Encoder encoder = Base64.getEncoder();

    private static final ObjectMapper mapper = new ObjectMapper();

    private static class ProtobufDecoder extends SimpleFunction<PubsubMessage, KV<String, String>> {
        private final ValueProvider<String> projectId;
        private final ValueProvider<String> schemaFileDescriptorSetFile;

        transient private static JsonFormat.TypeRegistry typeRegistry;
        transient private static final Map<String, Map<Integer, Message>> schemaClasses = new HashMap<>();

        private ProtobufDecoder(String projectId, ValueProvider<String> schemaFileDescriptorSetFile) {
            this.projectId = ValueProvider.StaticValueProvider.of(projectId);
            this.schemaFileDescriptorSetFile = schemaFileDescriptorSetFile;
        }

        private ProtobufDecoder(ValueProvider<String> projectId, ValueProvider<String> schemaFileDescriptorSet) {
            this.projectId = projectId;
            this.schemaFileDescriptorSetFile = schemaFileDescriptorSet;
        }

        private void ensureSchema() {
            if (typeRegistry != null) {
                return;
            }

            LOG.debug("schema not read yet, reading now");

            JsonFormat.TypeRegistry.Builder builder = JsonFormat.TypeRegistry.newBuilder()
                .add(Value.getDescriptor());

            ExtensionRegistry extensionRegistry = ExtensionRegistry.newInstance();
            Options.registerAllExtensions(extensionRegistry);

            int messagesCount = 0;

            try {
                String file = schemaFileDescriptorSetFile.get();
                if (file == null) {
                    LOG.error("schemaFileDescriptorSetFile can't be null");
                    throw new IllegalArgumentException("schemaFileDescriptorSetFile can't be null");
                }
                ResourceId resourceId = FileSystems.matchNewResource(file, false);
                ReadableByteChannel channel = FileSystems.open(resourceId);
                InputStream stream = Channels.newInputStream(channel);
                byte[] fileDescriptorSetBytes = stream.readAllBytes();
                DescriptorProtos.FileDescriptorSet fileDescriptorSet  = DescriptorProtos.FileDescriptorSet.parseFrom(fileDescriptorSetBytes, extensionRegistry);
                List<Descriptors.FileDescriptor> dependencies = new ArrayList<>();
                for (DescriptorProtos.FileDescriptorProto fileDescriptorProto : fileDescriptorSet.getFileList()) {
                    Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, dependencies.toArray(new Descriptors.FileDescriptor[0]), false);
                    dependencies.add(fileDescriptor);
                    for (Descriptors.Descriptor messageDescriptor : fileDescriptor.getMessageTypes()) {
                        // filter to only Hedwig messages
                        if (messageDescriptor.getOptions().hasExtension(Options.messageOptions)) {
                            DynamicMessage.Builder msg = DynamicMessage.newBuilder(messageDescriptor);
                            builder.add(messageDescriptor);
                            Options.MessageOptions msgOptions =
                                messageDescriptor.getOptions().getExtension(Options.messageOptions);
                            schemaClasses.putIfAbsent(msgOptions.getMessageType(), new HashMap<>());
                            schemaClasses.get(msgOptions.getMessageType())
                                .put(msgOptions.getMajorVersion(), msg.getDefaultInstanceForType());
                            ++messagesCount;
                        }
                    }
                }
                typeRegistry = builder.build();
            } catch (IOException|Descriptors.DescriptorValidationException e) {
                String msg = String.format("Unable to read schemaFileDescriptorSet at %s", schemaFileDescriptorSetFile.get());
                LOG.error(msg, e);
                throw new IllegalArgumentException(msg);
            }
            LOG.debug(String.format("Read %d Hedwig message types from schema", messagesCount));
        }

        /**
         * Pack an input message into container format and serialize for appropriate output into files. If the message
         * type is unknown, then packed data will be packed with unknown type, and thus may not decode unless you know
         * the correct type.
         *
         * @param data If message could be decoded, this must be set to the Message object.
         * @param groupId Group id for this message. If null, defaults to unknownMessageType.
         * @param input The input message
         * @return A tuple of group id and serialized message
         */
        private KV<String, String> packInContainer(Message data, String groupId, PubsubMessage input) {
            if (data == null) {
                // Type of data is unknown, so encode as base64
                Value.Builder dataBuilder;
                dataBuilder = Value.newBuilder();
                dataBuilder.setStringValue(encoder.encodeToString(input.getPayload()));
                data = dataBuilder.build();
            }

            if (groupId == null) {
                groupId = unknownMessageGroupId;
            }

            Container.PayloadV1 msg = null;
            if (input.getAttributeMap() != null) {
                Map<String, String> attributes = new HashMap<>(input.getAttributeMap());
                Container.MetadataV1.Builder metadataBuilder = Container.MetadataV1.newBuilder();
                String value = attributes.remove("hedwig_publisher");
                if (value != null) {
                    metadataBuilder.setPublisher(value);
                }
                value = attributes.remove("hedwig_message_timestamp");
                if (value != null) {
                    try {
                        long millis = Long.parseLong(value);
                        Timestamp timestamp = Timestamp.newBuilder().setSeconds(millis / 1000)
                            .setNanos((int) ((millis % 1000) * 1000000)).build();
                        metadataBuilder.setTimestamp(timestamp);
                    } catch (NumberFormatException ignored) {}
                }

                Container.PayloadV1.Builder builder = Container.PayloadV1.newBuilder();
                value = attributes.remove("hedwig_format_version");
                if (value != null) {
                    builder.setFormatVersion(value);
                }
                value = attributes.remove("hedwig_id");
                if (value != null) {
                    builder.setId(value);
                }
                value = attributes.remove("hedwig_schema");
                if (value != null) {
                    builder.setSchema(value);
                }
                metadataBuilder.putAllHeaders(attributes);
                builder.setMetadata(metadataBuilder);
                builder.setData(Any.pack(data));

                msg = builder.build();
            }

            String output;
            if (msg != null) {
                try {
                    output = JsonFormat.printer()
                        .omittingInsignificantWhitespace()
                        .preservingProtoFieldNames()
                        .usingTypeRegistry(typeRegistry)
                        .print(msg);
                    // XXX: workaround for https://github.com/protocolbuffers/protobuf/issues/7273
                    for (Map.Entry<String, String> replacement : HTML_SAFE_REPLACEMENT_CHARS.entrySet()) {
                        output = output.replaceAll(replacement.getKey(), replacement.getValue());
                    }
                } catch (InvalidProtocolBufferException e) {
                    // should never happen?
                    LOG.warn("Failed to convert to JSON for: {}", groupId);
                    if (data instanceof Value) {
                        // data is already an unknown value, serialization still failed: try with plain JSON serializer
                        try {
                            output = mapper.writeValueAsString(msg);
                        } catch (JsonProcessingException jsonProcessingException) {
                            // still failed! booo.. last fallback, hand written JSON:
                            output = String.format("{" +
                                    "\"pubsub_message_id\":\"%s\"," +
                                    "\"message_type\":\"%s\"," +
                                    "\"payload\":\"%s\"," +
                                    "\"error\":\"Failed to serialize\"" +
                                    "}",
                                input.getMessageId(),
                                groupId,
                                encoder.encodeToString(input.getPayload())
                            );
                        }
                    } else {
                        return this.packInContainer(null, groupId, input);
                    }
                }
            } else {
                output = String.format("{" +
                        "\"pubsub_message_id\":\"%s\"," +
                        "\"message_type\":\"%s\"," +
                        "\"payload\":\"%s\"," +
                        "\"error\":\"No attributes found, can't decode data\"" +
                        "}",
                    input.getMessageId(),
                    groupId,
                    encoder.encodeToString(input.getPayload())
                );
            }
            return KV.of(String.format("%s/%s", projectId.get(), groupId), output);
        }

        private KV<String, String> packInContainer(PubsubMessage input) {
            return this.packInContainer(null, null, input);
        }

        private KV<String, String> packInContainer(String groupId, PubsubMessage input) {
            return this.packInContainer(null, groupId, input);
        }

        private String groupId(String messageType, int majorVersion) {
            // this value is also used as file prefix later in the pipeline
            return String
                .format("%s-v%s", messageType, majorVersion)
                .replaceAll("[._]", "-");
        }

        @Override
        public KV<String, String> apply(PubsubMessage input) {
            // assume that transport message attributes are in use
            // attributes that must be set already, if not set that's a fail

            ensureSchema();

            String schema = input.getAttribute("hedwig_schema");
            if (schema == null || schema.equals("")) {
                LOG.warn("No schema found, fallback to binary encoding with unknown type");
                return this.packInContainer(input);
            }

            Matcher matcher = schemaPattern.matcher(schema);
            if (!matcher.matches()) {
                LOG.warn("Invalid schema found: {}, fallback to binary encoding with unknown type", schema);
                return this.packInContainer(input);
            }

            String messageType = matcher.group(1);
            int majorVersion = Integer.parseInt(matcher.group(2));
            int minorVersion = Integer.parseInt(matcher.group(3));
            String groupId = this.groupId(messageType, majorVersion);

            Message protoMessage = schemaClasses.getOrDefault(messageType, emptyMap).get(majorVersion);
            if (protoMessage == null) {
                LOG.warn(
                    "Proto message not found for in map: {} v{}, fallback to binary encoding", messageType, majorVersion);
                return this.packInContainer(groupId, input);
            }

            Message msg;
            try {
                msg = protoMessage.getParserForType().parseFrom(input.getPayload());
            } catch (InvalidProtocolBufferException e) {
                LOG.warn("Failed to parse payload for: {} v{}", messageType, majorVersion);
                return this.packInContainer(groupId, input);
            }

            Options.MessageOptions msgOptions =
                msg.getDescriptorForType().getOptions().getExtension(Options.messageOptions);
            if (msgOptions.getMinorVersion() < minorVersion) {
                LOG.warn(
                    "Known proto class but unknown minor version: {} v{}.{}", messageType, majorVersion, minorVersion);
                // encode without decoding payload because we don't want to lose unknown fields added by minor versions
                return this.packInContainer(groupId, input);
            }

            KV<String, String> output = this.packInContainer(msg, groupId, input);

            LOG.debug("Successfully parsed and encoded as JSON: {} v{}", messageType, majorVersion);
            return output;
        }
    }

    /**
     * Runs the pipeline with the supplied options.
     * @param options runtime options
     */
    private static PipelineResult run(RuntimeOptions options) {
        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        /*
         * Steps:
         *  1) For each subscription:
         *      a) Read proto messages from PubSub
         *      b) Decode proto messages and re-encode in human readable format
         *  2) Window the messages into minute intervals specified by the executor.
         *  3) Output the windowed files to GCS
         */

        // Read PubSub subscriptions and map messages with group id
        List<PCollection<KV<String, String>>> pCollections = new ArrayList<>();
        for (String subName : options.getInputSubscriptions()) {
            Matcher matcher = SUBSCRIPTION_REGEXP.matcher(subName);
            if (!matcher.matches()) {
                throw new RuntimeException(String.format(
                    "Invalid subscription: %s, should have matched: %s%n", subName, SUBSCRIPTION_REGEXP));
            }
            String topicName = matcher.group(1);
            String subId = String.format("projects/%s/subscriptions/hedwig-firehose-%s", options.getProject(), topicName);
            PCollection<KV<String, String>> messages = pipeline
                .apply("Read " + topicName,
                    PubsubIO.readMessagesWithAttributesAndMessageId().fromSubscription(subId))
                .apply("Decode " + topicName, MapElements.via(new ProtobufDecoder(options.getProject(), options.getSchemaFileDescriptorSetFile())));
            pCollections.add(messages);
        }

        // Read PubSub subscriptions and map messages with group id
        for (String subIdCrossProject : options.getInputSubscriptionsCrossProject()) {
            Matcher matcher = SUBSCRIPTION_CROSS_PROJECT_REGEXP.matcher(subIdCrossProject);
            if (!matcher.matches()) {
                throw new RuntimeException(String.format(
                    "Invalid subscription: %s, should have matched: %s%n", subIdCrossProject, SUBSCRIPTION_CROSS_PROJECT_REGEXP));
            }
            String otherProjectId = matcher.group(1);
            String topicName = matcher.group(2);
            String subId = String.format("projects/%s/subscriptions/hedwig-firehose-%s-%s", options.getProject(), otherProjectId, topicName);
            String subDisplayName = String.format("%s-%s", otherProjectId, topicName);
            PCollection<KV<String, String>> messages = pipeline
                .apply("Read " + subDisplayName,
                    PubsubIO.readMessagesWithAttributesAndMessageId().fromSubscription(subId))
                .apply("Decode " + subDisplayName, MapElements.via(new ProtobufDecoder(otherProjectId, options.getSchemaFileDescriptorSetFile())));
            pCollections.add(messages);
        }

        PCollectionList.of(pCollections)
            .apply("Flatten", Flatten.pCollections())
            .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .apply(
                options.getWindowDuration() + " Window",
                Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))

            // Apply windowed file writes. Use a NestedValueProvider because the filename
            // policy requires a resourceId generated from the input value at runtime.
            .apply(
                "Write File(s)",
                FileIO.<String, KV<String, String>>writeDynamic()
                .withDestinationCoder(StringUtf8Coder.of())
                .by(KV::getKey)
                .via(Contextful.fn(KV::getValue), TextIO.sink())
                .withNumShards(options.getNumShards())
                .to(options.getOutputDirectory())
                .withNaming(DatePartitionedName::new)
                .withTempDirectory(ValueProvider.NestedValueProvider.of(
                    maybeUseUserTempLocation(
                        options.getUserTempLocation(),
                        options.getOutputDirectory()),
                    input -> FileBasedSink.convertToFileResourceIfPossible(input).toString()))
                .withCompression(Compression.GZIP));

        // Execute the pipeline and return the result.
        return pipeline.run();
    }

    public static void main(String[] args) throws IllegalArgumentException {
        PipelineOptionsFactory.register(RuntimeOptions.class);

        RuntimeOptions options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(RuntimeOptions.class);

        options.setJobName("hedwig-firehose");
        options.setStreaming(true);
        options.setRunner(DataflowRunner.class);

        PipelineResult result = Firehose.run(options);
        // ignore result?
        System.exit(0);
    }

    /**
     * Utility method for using optional parameter userTempLocation as TempDirectory.
     * This is useful when output bucket is locked and temporary data cannot be deleted.
     *
     * @param userTempLocation user provided temp location
     * @param outputLocation user provided outputDirectory to be used as the default temp location
     * @return userTempLocation if available, otherwise outputLocation is returned.
     */
    private static ValueProvider<String> maybeUseUserTempLocation(
        ValueProvider<String> userTempLocation,
        ValueProvider<String> outputLocation) {
        return DualInputNestedValueProvider.of(
            userTempLocation,
            outputLocation,
            (SerializableFunction<DualInputNestedValueProvider.TranslatorInput<String, String>, String>)
                input -> (input.getX() != null) ? input.getX() : input.getY());
    }
}
