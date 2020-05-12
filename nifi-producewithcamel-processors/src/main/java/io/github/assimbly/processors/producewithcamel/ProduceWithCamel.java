/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.assimbly.processors.producewithcamel;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.camel.ProducerTemplate;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import java.util.UUID;

import java.io.IOException;
import java.io.InputStream;
import org.apache.nifi.processor.io.InputStreamCallback;

import org.assimbly.connector.Connector;
import org.assimbly.docconverter.DocConverter;


@Tags({"Camel Producer"})
@CapabilityDescription("Produce messages with Apache Camel")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ProduceWithCamel extends AbstractProcessor {
	
    public static final PropertyDescriptor TO_URI = new PropertyDescriptor
            .Builder().name("TO_URI")
            .displayName("To URI")
            .description("To endpoint. Producess message with the specified Camel component")
            .required(false)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .build();

    public static final PropertyDescriptor ERROR_URI = new PropertyDescriptor
            .Builder().name("ERROR_URI")
            .displayName("Error URI")
            .description("Error endpoint. Sends errors with the specified Camel component")
            .required(false)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAXIMUM_REDELIVERIES = new PropertyDescriptor
            .Builder().name("MAXIMUM_REDELIVERIES")
            .displayName("Maximum Redelivery")
            .description("Number of redeliveries on failure")
            .defaultValue("0")
            .required(true)            
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor REDELIVERY_DELAY = new PropertyDescriptor
            .Builder().name("REDELIVERY_DELAY")
            .displayName("Redelivery delay")
            .description("Delay in ms between redeliveries")
            .defaultValue("3000")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor LOG_LEVEL = new PropertyDescriptor
            .Builder().name("LOG_LEVEL")
            .displayName("LOG_LEVEL")
            .description("Set the log level")
            .required(true)
            .defaultValue("OFF")
            .allowableValues("OFF","INFO","WARN","ERROR","DEBUG","TRACE")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();	    
	   
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Succes relationship")
            .build();
    
    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private Connector connector = new org.assimbly.connector.impl.CamelConnector();
    
    private TreeMap<String, String> properties;
    
    private ProducerTemplate template;
    
    private String input;
    
    private String flowId;
    
    
    @Override
    protected void init(final ProcessorInitializationContext context) {

		getLogger().info("Init process..............................");

    	final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(TO_URI);
        descriptors.add(ERROR_URI);
        descriptors.add(MAXIMUM_REDELIVERIES);
        descriptors.add(REDELIVERY_DELAY);
        descriptors.add(LOG_LEVEL);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
        
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

       
    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    	//Use Assimbly Connector to manage Apache Camel (https://github.com/assimbly/connector)
    	getLogger().info("Starting Apache Camel");
        
        //Start Apache camel
        try {
			startCamelConnector();
		} catch (Exception e2) {
			getLogger().error("Can't start Apache Camel.");
			e2.printStackTrace();
		}
        
		//Create a flow ID
		UUID uuid = UUID.randomUUID();
        flowId = context.getName() + uuid.toString();

        
   		//configure the flow (Camel route)
        try {
			configureCamelFlow(context);
		} catch (Exception e1) {
			getLogger().error("Can't configure Apache Camel route.");
			e1.printStackTrace();
		}
        
        
   		//start the flow (Camel route)
        try {
			connector.startFlow(flowId);
		} catch (Exception e1) {
			getLogger().error("Can't start Apache Camel.");
			e1.printStackTrace();
		}        
        
        
   		//Create the endpoint producer
   		try {
			template = connector.getProducerTemplate();
	   		template.setDefaultEndpointUri("direct:nifi-" + flowId);
	   		template.start();

		} catch (Exception e) {
			getLogger().error("Can't create Apache Camel endpoint.");
			e.printStackTrace();
		}
        
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        
    	FlowFile flowfile = session.get();
  
        if ( flowfile == null ) {
            return;
        }

        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try{

                	//Convert flowFile to a string
                	input = DocConverter.convertStreamToString(in);

                	//Send the message to the Camel route
                	template.sendBody(input); 

                }catch(Exception ex){
                    ex.printStackTrace();
                    getLogger().error("Failed to send flowFile to Camel.");
                }
            }
        });

        session.transfer(flowfile, SUCCESS);
        
    }


    
    public void startCamelConnector() throws Exception {

		getLogger().info("Starting Apache Camel");

		//Start Camel context
   		connector.start();
	
    }
    
    @OnStopped
    public void stopCamelConnector() throws Exception {

		getLogger().info("Stopping Apache Camel");

    	connector.stopFlow(flowId);
    	connector.stop();
    	template.stop();
    	
    }
    
    
    private void configureCamelFlow(final ProcessContext context) throws Exception{
    	
		String fromURIProperty = "direct:nifi-" + flowId;
		String toURIProperty = context.getProperty(TO_URI).getValue();
    	String errorURIProperty = context.getProperty(ERROR_URI).getValue();
        final String maximumRedeliveriesProperty = context.getProperty(MAXIMUM_REDELIVERIES).getValue();
    	final String redeliveryDelayProperty = context.getProperty(REDELIVERY_DELAY).getValue();
    	final String logLevelProperty = context.getProperty(LOG_LEVEL).getValue();
    	
    	if(errorURIProperty == null || errorURIProperty.isEmpty()) {
    		errorURIProperty = "log:ProduceWithCamel. + flowId + ?level=OFF&showAll=true&multiline=true&style=Fixed";
    	}
		
     	properties = new TreeMap<>();
     	
		properties.put("id",flowId);	
		properties.put("flow.name","camelroute-" + flowId);	
		properties.put("flow.type","default");

		properties.put("flow.maximumRedeliveries",maximumRedeliveriesProperty);
		properties.put("flow.redeliveryDelay",redeliveryDelayProperty);
		properties.put("flow.logLevel",logLevelProperty);
		properties.put("flow.offloading","false");
		
		properties.put("from.uri", fromURIProperty);
		properties.put("to.1.uri", toURIProperty);
		properties.put("error.uri", errorURIProperty);
				
		properties.put("offramp.uri.list", "direct:flow=" + flowId + "endpoint=1");

		connector.setFlowConfiguration(properties);

    }
    
}
