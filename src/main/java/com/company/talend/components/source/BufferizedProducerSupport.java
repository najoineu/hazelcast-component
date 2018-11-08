package com.company.talend.components.source;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.bind.Jsonb;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;

public class BufferizedProducerSupport<T> {
	
	private  Supplier<Iterator<T>> supplier;

    private Iterator<T> current;
     
   
    	   public BufferizedProducerSupport() {
    			super();
    			  			
    		}
    

	public T next() {
        if (current == null || !current.hasNext()) {
            current = supplier.get();
        }
        return current != null && current.hasNext() ? current.next() : null;
}

	public Iterator<JsonObject> test(Iterator<Member> memberIterators, InputMapperConfiguration configuration, HazelcastInstance instance,
			Jsonb jsonb, JsonBuilderFactory jsonFactory){
		
		if (!memberIterators.hasNext()) {
            return null;
        }
        final Member member = memberIterators.next();
        // note: this works if this jar is deployed on the hz cluster
        try {
            return instance.getExecutorService(configuration.getExecutorService())
                    .submitToMember(new SerializableTask<Map<String, String>>() {

                        @Override
                        public Map<String, String> call() throws Exception {
                            final IMap<Object, Object> map = localInstance.getMap(configuration.getMapName());
                            final Set<?> keys = map.localKeySet();
                            return keys.stream().collect(Collectors.toMap(jsonb::toJson, e -> jsonb.toJson(map.get(e))));
                        }
                    }, member).get(configuration.getTimeout(), TimeUnit.SECONDS).entrySet().stream()
                    .map(entry -> {
                        final JsonObjectBuilder builder = jsonFactory.createObjectBuilder();
                        if (entry.getKey().startsWith("{")) {
                            builder.add("key", jsonb.fromJson(entry.getKey(), JsonObject.class));
                        } else { // plain string
                            builder.add("key", entry.getKey());
                        }
                        if (entry.getValue().startsWith("{")) {
                            builder.add("value", jsonb.fromJson(entry.getValue(), JsonObject.class));
                        } else { // plain string
                            builder.add("value", entry.getValue());
                        }
                        return builder.build();
                    })
                    .collect(Collectors.toList())
                    .iterator();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (final ExecutionException | TimeoutException e) {
            throw new IllegalArgumentException(e);
        }
		
	}
	
}
