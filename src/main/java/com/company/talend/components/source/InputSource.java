package com.company.talend.components.source;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.bind.Jsonb;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;

import com.company.talend.components.service.Hazelcast_componentService;
import com.company.talend.components.source.BufferizedProducerSupport;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;

@Documentation("TODO fill the documentation for this source")
public class InputSource implements Serializable {
    private final InputMapperConfiguration configuration;
    private final Hazelcast_componentService service;
    private final JsonBuilderFactory jsonFactory;
    private final Jsonb jsonb;
    private final Collection<String> members;
    private transient HazelcastInstance instance;
//    private transient JsonObject buffer;
    private transient BufferizedProducerSupport<JsonObject> buffer ;

    public InputSource(@Option("configuration") final InputMapperConfiguration configuration,
                        final Hazelcast_componentService service,
                        final JsonBuilderFactory jsonFactory,
                        final Jsonb jsonb) {
//        this.configuration = configuration;
//        this.service = service;
//        this.jsonFactory = jsonFactory;
    	this(configuration, jsonFactory, jsonb, service, Collections.emptyList());
    }

    public InputSource(InputMapperConfiguration configuration, JsonBuilderFactory jsonFactory, Jsonb jsonb,
			Hazelcast_componentService service, Collection<String> members) {
		// TODO Auto-generated constructor stub
    	this.configuration = configuration;
        this.jsonFactory = jsonFactory;
        this.jsonb = jsonb;
        this.service = service;
        this.members = members;
	}

    
/*    public static class BufferizedProducerSupport<JsonObject> {
        public BufferizedProducerSupport() {}
    }*/
    
   /* public class BufferizedProducerSupport<T> {

        private  Supplier<Iterator<T>> supplier;

        private Iterator<T> current;

     
        
        public T next() {
            if (current == null || !current.hasNext()) {
                current = supplier.get();
            }
            return current != null && current.hasNext() ? current.next() : null;
        }
    }
*/
    
	@PostConstruct
    public void init() throws IOException {
        // this method will be executed once for the whole component execution,
        // this is where you can establish a connection for instance
        instance = service.findInstance(configuration.newConfig());
        final Iterator<Member> memberIterators = instance.getCluster().getMembers().stream()
                .filter(m -> members.isEmpty() || members.contains(m.getUuid()))
                .collect(Collectors.toSet())
                .iterator();
        BufferizedProducerSupport<JsonObject> buffer = new BufferizedProducerSupport<>();
        buffer.test(memberIterators, configuration, instance, jsonb, jsonFactory);
        }
 
	

    @Producer
    public JsonObject  next() {
        // this is the method allowing you to go through the dataset associated
        // to the component configuration
        //
        // return null means the dataset has no more data to go through
        // you can use the builderFactory to create a new Record.
        return buffer.next();
    }

    @PreDestroy
    public void release() {
        // this is the symmetric method of the init() one,
        // release potential connections you created or data you cached
    	instance.getLifecycleService().shutdown();
    }
}