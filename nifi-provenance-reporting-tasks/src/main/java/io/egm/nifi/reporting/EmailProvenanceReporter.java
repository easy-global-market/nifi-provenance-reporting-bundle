package io.egm.nifi.reporting;

import jakarta.mail.*;
import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeUtility;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.ReportingContext;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Tags({"email", "provenance", "smtp"})
@CapabilityDescription("Sends an e-mail when a provenance event is considered as an error")
public class EmailProvenanceReporter extends AbstractProvenanceReporter {

    public static final PropertyDescriptor SMTP_HOSTNAME = new PropertyDescriptor.Builder()
            .name("SMTP Hostname")
            .description("The hostname of the SMTP host")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SMTP_PORT = new PropertyDescriptor.Builder()
            .name("SMTP Port")
            .description("The port used for SMTP communications")
            .required(true)
            .defaultValue("25")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor SMTP_AUTH = new PropertyDescriptor.Builder()
            .name("SMTP Auth")
            .description("Flag indicating whether authentication should be used")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor SMTP_USERNAME = new PropertyDescriptor.Builder()
            .name("SMTP Username")
            .description("Username for the SMTP account")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor SMTP_PASSWORD = new PropertyDescriptor.Builder()
            .name("SMTP Password")
            .description("Password for the SMTP account")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor SMTP_TLS = new PropertyDescriptor.Builder()
            .name("SMTP TLS")
            .displayName("SMTP STARTTLS")
            .description("Flag indicating whether Opportunistic TLS should be enabled using STARTTLS command")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor SMTP_SOCKET_FACTORY = new PropertyDescriptor.Builder()
            .name("SMTP Socket Factory")
            .description("Socket Factory to use for SMTP Connection")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("javax.net.ssl.SSLSocketFactory")
            .build();
    public static final PropertyDescriptor HEADER_XMAILER = new PropertyDescriptor.Builder()
            .name("SMTP X-Mailer Header")
            .description("X-Mailer used in the header of the outgoing email")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("NiFi")
            .build();

    public static final PropertyDescriptor CONTENT_TYPE = new PropertyDescriptor.Builder()
            .name("Content Type")
            .description("Mime Type used to interpret the contents of the email, such as text/plain or text/html")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("text/plain")
            .build();
    public static final PropertyDescriptor FROM = new PropertyDescriptor.Builder()
            .name("From")
            .description("Specifies the Email address to use as the sender. "
                    + "Comma separated sequence of addresses following RFC822 syntax.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor TO = new PropertyDescriptor.Builder()
            .name("To")
            .description("The recipients to include in the To-Line of the email. "
                    + "Comma separated sequence of addresses following RFC822 syntax.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor CC = new PropertyDescriptor.Builder()
            .name("CC")
            .description("The recipients to include in the CC-Line of the email. "
                    + "Comma separated sequence of addresses following RFC822 syntax.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor BCC = new PropertyDescriptor.Builder()
            .name("BCC")
            .description("The recipients to include in the BCC-Line of the email. "
                    + "Comma separated sequence of addresses following RFC822 syntax.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor SPECIFIC_RECIPIENT_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Specific Recipient Attribute Name")
            .displayName("Specific Recipient Attribute Name")
            .description("The name of the attribute that contains a specific email to receive alerts for this flow only. ")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor INPUT_CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("input-character-set")
            .displayName("Input Character Set")
            .description("Specifies the character set of the FlowFile contents "
                    + "for reading input FlowFile contents to generate the message body "
                    + "or as an attachment to the message. "
                    + "If not set, UTF-8 will be the default value.")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue(StandardCharsets.UTF_8.name())
            .build();
    public static final PropertyDescriptor EMAIL_SUBJECT_PREFIX = new PropertyDescriptor.Builder()
            .name("Email Subject Prefix")
            .displayName("Email Subject Prefix")
            .description("Prefix to be added in the email subject")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor GROUP_SIMILAR_ERRORS = new PropertyDescriptor.Builder()
            .name("Group Similar Errors")
            .displayName("Group Similar Errors")
            .description("Specifies whether to group similar error events into a single email or not. " +
                    "Set to true to receive a single email with grouped errors. " +
                    "Set to false to receive an email for each error. " +
                    "The grouping is done by processor id and error information (event type and details)")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = super.getCommonPropertyDescriptors();
        descriptors.add(SMTP_HOSTNAME);
        descriptors.add(SMTP_PORT);
        descriptors.add(SMTP_AUTH);
        descriptors.add(SMTP_USERNAME);
        descriptors.add(SMTP_PASSWORD);
        descriptors.add(SMTP_TLS);
        descriptors.add(SMTP_SOCKET_FACTORY);
        descriptors.add(HEADER_XMAILER);
        descriptors.add(CONTENT_TYPE);
        descriptors.add(FROM);
        descriptors.add(TO);
        descriptors.add(CC);
        descriptors.add(BCC);
        descriptors.add(SPECIFIC_RECIPIENT_ATTRIBUTE_NAME);
        descriptors.add(INPUT_CHARACTER_SET);
        descriptors.add(EMAIL_SUBJECT_PREFIX);
        descriptors.add(GROUP_SIMILAR_ERRORS);

        return descriptors;
    }

    /**
     * Mapping of the mail properties to the NiFi PropertyDescriptors that will be evaluated at runtime
     */
    private static final Map<String, PropertyDescriptor> propertyToContext = new HashMap<>();

    static {
        propertyToContext.put("mail.smtp.host", SMTP_HOSTNAME);
        propertyToContext.put("mail.smtp.port", SMTP_PORT);
        propertyToContext.put("mail.smtp.socketFactory.port", SMTP_PORT);
        propertyToContext.put("mail.smtp.socketFactory.class", SMTP_SOCKET_FACTORY);
        propertyToContext.put("mail.smtp.auth", SMTP_AUTH);
        propertyToContext.put("mail.smtp.starttls.enable", SMTP_TLS);
        propertyToContext.put("mail.smtp.user", SMTP_USERNAME);
        propertyToContext.put("mail.smtp.password", SMTP_PASSWORD);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> errors = new ArrayList<>(super.customValidate(context));

        final String to = context.getProperty(TO).getValue();
        final String cc = context.getProperty(CC).getValue();
        final String bcc = context.getProperty(BCC).getValue();

        if (to == null && cc == null && bcc == null) {
            errors.add(new ValidationResult.Builder().subject("To, CC, BCC").valid(false).explanation("Must specify at least one To/CC/BCC address").build());
        }

        return errors;
    }

    private void setMessageHeader(final String header, final String value, final Message message) throws MessagingException {
        try {
            message.setHeader(header, MimeUtility.encodeText(value));
        } catch (UnsupportedEncodingException e) {
            getLogger().warn("Unable to add header {} with value {} due to encoding exception", header, value);
        }
    }

    private Properties getEmailProperties(ReportingContext context) {
        final Properties properties = new Properties();

        for (final Map.Entry<String, PropertyDescriptor> entry : propertyToContext.entrySet()) {
            // Evaluate the property descriptor against the flow file
            final String propertyValue = context.getProperty(entry.getValue()).getValue();
            final String property = entry.getKey();

            // Nullable values are not allowed, so filter out
            if (null != propertyValue) {
                properties.setProperty(property, propertyValue);
            }
        }

        return properties;
    }

    /**
     * Based on the input properties, determine whether an authenticate or unauthenticated session should be used.
     * If authenticated, creates a Password Authenticator for use in sending the email.
     */
    private Session createMailSession(final Properties properties, ReportingContext context) {
        final boolean auth = Boolean.parseBoolean(context.getProperty(SMTP_AUTH).getValue());

        /*
         * Conditionally create a password authenticator if the 'auth' parameter is set.
         */
        return auth ? Session.getInstance(properties, new Authenticator() {
            @Override
            public PasswordAuthentication getPasswordAuthentication() {
                final String username = properties.getProperty("mail.smtp.user");
                final String password = properties.getProperty("mail.smtp.password");
                return new PasswordAuthentication(username, password);
            }
        }) : Session.getInstance(properties); // without auth
    }

    /**
     * @param context            the current context
     * @param propertyDescriptor the property to evaluate
     * @return an InternetAddress[] parsed from the supplied property
     * @throws AddressException if the property cannot be parsed to a valid InternetAddress[]
     */
    private InternetAddress[] toInetAddresses(final ReportingContext context, PropertyDescriptor propertyDescriptor)
            throws AddressException {
        InternetAddress[] parse;
        final String value = context.getProperty(propertyDescriptor).getValue();

        if (value == null || value.isEmpty()) {
            if (propertyDescriptor.isRequired()) {
                final String exceptionMsg = "Required property '" + propertyDescriptor.getDisplayName() + "' evaluates to an empty string.";
                throw new AddressException(exceptionMsg);
            } else {
                parse = new InternetAddress[0];
            }
        } else {
            try {
                parse = InternetAddress.parse(value);
            } catch (AddressException e) {
                final String exceptionMsg = "Unable to parse a valid address for property '" + propertyDescriptor.getDisplayName() + "' with value '" + value + "'";
                throw new AddressException(exceptionMsg);
            }
        }

        return parse;
    }

    private InternetAddress[] attributeValueToInetAddress(final ReportingContext context, String specificRecipientAttributeValue, PropertyDescriptor propertyDescriptor) {
        InternetAddress[] parse = new InternetAddress[0];
        try {
            parse = InternetAddress.parse(specificRecipientAttributeValue);
        } catch (AddressException e) {
            getLogger().error("Unable to parse a valid address for property '"
                    + propertyDescriptor.getDisplayName() + "'from the attribute '"
                    + context.getProperty(propertyDescriptor).getValue()
                    + "' with value '" + specificRecipientAttributeValue + "'");
        }
        return parse;
    }

    /**
     * Utility function to get a charset from the {@code INPUT_CHARACTER_SET} property
     *
     * @param context the ProcessContext
     * @return the Charset
     */
    private Charset getCharset(final ReportingContext context) {
        return Charset.forName(context.getProperty(INPUT_CHARACTER_SET).getValue());
    }

    private String getSpecificRecipientValue(final ReportingContext context, final Map<String, Object> event) {
        final String specificRecipientAttributeName = context.getProperty(SPECIFIC_RECIPIENT_ATTRIBUTE_NAME).getValue();
        Map<String, String> previousAttributes =
            (Map<String, String>) event.getOrDefault("previous_attributes", new HashMap<>());
        if (previousAttributes.get(specificRecipientAttributeName) != null) {
            return previousAttributes.get(specificRecipientAttributeName);
        } else {
            Map<String, String> updatedAttributes =
                (Map<String, String>) event.getOrDefault("updated_attributes", new HashMap<>());
            return updatedAttributes.get(specificRecipientAttributeName);
        }
    }

    private String composeMessageContent(final Map<String, Object> event, Boolean groupSimilarErrors, int groupedEventsSize) {
        final StringBuilder message = new StringBuilder();

        message.append("Affected processor:\n")
            .append("\tProcessor name: ").append(event.get("component_name")).append("\n")
            .append("\tProcessor type: ").append(event.get("component_type")).append("\n")
            .append("\tProcess group: ").append(event.get("process_group_name")).append("\n");

        if (groupSimilarErrors && groupedEventsSize > 1) {
            message.append("\tTotal similar errors : ").append(groupedEventsSize).append("\n");
        }

        message.append("\tURL: ").append(event.get("component_url")).append("\n");

        message.append("\n");
        message.append("Error information:\n")
            .append("\tDetails: ").append(event.get("details")).append("\n")
            .append("\tEvent type: ").append(event.get("event_type")).append("\n");

        if (event.containsKey("updated_attributes")) {
            Map<String, String> updatedAttributes = (Map<String, String>) event.get("updated_attributes");
            message.append("\nFlow file - Updated attributes:\n");
            updatedAttributes.keySet().stream().sorted().forEach(attributeName ->
                message.append(String.format("\t%1$s: %2$s\n", attributeName, updatedAttributes.get(attributeName)))
            );
        }

        if (event.containsKey("previous_attributes")) {
            Map<String, String> previousAttributes = (Map<String, String>) event.get("previous_attributes");
            message.append("\nFlow file - Previous attributes:\n");
            previousAttributes.keySet().stream().sorted().forEach(attributeName ->
                    message.append(String.format("\t%1$s: %2$s\n", attributeName, previousAttributes.get(attributeName)))
            );
        }

        message.append("\nFlow file - content:\n")
            .append("\tDownload input: ").append(event.get("download_input_content_uri")).append("\n")
            .append("\tDownload output: ").append(event.get("download_output_content_uri")).append("\n")
            .append("\tView input: ").append(event.get("view_input_content_uri")).append("\n")
            .append("\tView output: ").append(event.get("view_output_content_uri")).append("\n");

        message.append("\n");
        return message.toString();
    }

    @Override
    public void indexEvents(final List<Map<String, Object>> events, final ReportingContext context) {
        List<Map<String, Object>> errorEvents = filterErrorEvents(events);

        if (context.getProperty(GROUP_SIMILAR_ERRORS).asBoolean()) {
            // Group all error events to send in a single batch email
            errorEvents.stream()
                .collect(Collectors.groupingBy(this::groupingKeys))
                .forEach((groupingKeys, groupedEvents) -> {
                    try {
                        sendErrorEmail(groupedEvents.get(0), context, groupedEvents.size());
                    } catch (MessagingException e) {
                        getLogger().error("Error sending error email: " + e.getMessage(), e);
                    }
                });
        } else {
            // Send individual emails for each error event
            for (Map<String, Object> event : errorEvents) {
                try {
                    sendErrorEmail(event, context, 0);
                } catch (MessagingException e) {
                    getLogger().error("Error sending error email: " + e.getMessage(), e);
                }
            }
        }
    }

    private List<Map<String, Object>> filterErrorEvents(final List<Map<String, Object>> events) {
        return events.stream()
            .filter(event -> "Error".equals(event.get("status")))
            .collect(Collectors.toList());
    }


    private Map<String, String> groupingKeys(Map<String, Object> event) {
        return Map.of(
            "component_id", event.get("component_id").toString(),
            "details", event.get("details").toString(),
            "event_type", event.get("event_type").toString()
        );
    }

    public void sendErrorEmail(Map<String, Object> event, ReportingContext context, int groupedEventsSize) throws MessagingException {

        String subjectPrefix = context.getProperty(EMAIL_SUBJECT_PREFIX).getValue();
        Boolean groupSimilarErrors = context.getProperty(GROUP_SIMILAR_ERRORS).asBoolean();
        StringBuilder emailSubjectBuilder = new StringBuilder();

        if (subjectPrefix != null) {
            emailSubjectBuilder.append("[").append(subjectPrefix).append("] ");
        }

        if (groupSimilarErrors && groupedEventsSize > 1) {
            emailSubjectBuilder.append(groupedEventsSize).append(" errors occurred in processor ")
                .append(event.get("component_name")).append(" in process group ")
                .append(event.get("process_group_name"));

        } else {
            emailSubjectBuilder.append("Error occurred in processor ")
                .append(event.get("component_name")).append(" in process group ")
                .append(event.get("process_group_name"));
        }
        String emailSubject = emailSubjectBuilder.toString();

        final Properties properties = this.getEmailProperties(context);
        final Session mailSession = this.createMailSession(properties, context);
        final Message message = new MimeMessage(mailSession);
        InternetAddress[] inetAddressesArray;
        String specificRecipientAttributeValue = null;
        if (context.getProperty(SPECIFIC_RECIPIENT_ATTRIBUTE_NAME).getValue() != null) {
            specificRecipientAttributeValue = getSpecificRecipientValue(context, event);
        }

        if (specificRecipientAttributeValue != null) {
            inetAddressesArray = Stream.concat(
                    Arrays.stream(toInetAddresses(context, TO)),
                    Arrays.stream(attributeValueToInetAddress(context, specificRecipientAttributeValue, SPECIFIC_RECIPIENT_ATTRIBUTE_NAME))
            ).toArray(InternetAddress[]::new);
        } else {
            inetAddressesArray = toInetAddresses(context, TO);
        }

        try {
            message.addFrom(toInetAddresses(context, FROM));
            message.setRecipients(Message.RecipientType.TO, inetAddressesArray);
            message.setRecipients(Message.RecipientType.CC, toInetAddresses(context, CC));
            message.setRecipients(Message.RecipientType.BCC, toInetAddresses(context, BCC));
            this.setMessageHeader("X-Mailer", context.getProperty(HEADER_XMAILER).getValue(), message);
            message.setSubject(emailSubject);

            final String messageText = composeMessageContent(event, groupSimilarErrors, groupedEventsSize);

            final String contentType = context.getProperty(CONTENT_TYPE).getValue();
            final Charset charset = getCharset(context);

            message.setContent(messageText, contentType + String.format("; charset=\"%s\"", MimeUtility.mimeCharset(charset.name())));

            message.setSentDate(new Date());

            // message is not a Multipart, need to set Content-Transfer-Encoding header at the message level
            message.setHeader("Content-Transfer-Encoding", MimeUtility.getEncoding(message.getDataHandler()));

            // Send the message
            message.saveChanges();
            send(message);
            getLogger().debug("Error email for provenance event sent successfully!");
        } catch (MessagingException e) {
            getLogger().error("Failed to send error email for provenance event. Error details: " + e.getMessage());
        }
    }

    /**
     * Wrapper for static method {@link Transport#send(Message)} to add testability of this class.
     *
     * @param msg the message to send
     * @throws MessagingException on error
     */
    protected void send(final Message msg) throws MessagingException {
        Transport.send(msg);
    }
}
