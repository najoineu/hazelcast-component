package com.company.talend.components.source;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonBuilderFactory;
import javax.json.bind.Jsonb;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.meta.Documentation;

import com.company.talend.components.service.Hazelcast_componentService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;

//
// this class role is to enable the work to be distributed in environments supporting it.
//
@Version(1) // default version is 1, if some configuration changes happen between 2 versions you can add a migrationHandler
@Icon(Icon.IconType.STAR) // you can use a custom one using @Icon(value=CUSTOM, custom="filename") and adding icons/filename_icon32.png in resources
@PartitionMapper(name = "Input")
@Documentation("TODO fill the documentation for this mapper")
public class InputMapper implements Serializable {
    private final InputMapperConfiguration configuration;
    private final Hazelcast_componentService service;
    private final JsonBuilderFactory jsonFactory;
    private final Jsonb jsonb;
    private final Collection<String> members;
    private transient HazelcastInstance instance;
    private transient IExecutorService executorService; 

    public InputMapper(@Option("configuration") final InputMapperConfiguration configuration,
                        final Hazelcast_componentService service,
                        final JsonBuilderFactory jsonFactory,
                        final Jsonb jsonb) {
//        this.configuration = configuration;
//        this.service = service;
//        this.jsonFactory = jsonFactory;
//        this.jsonb = jsonb;
    	this(configuration, jsonFactory, jsonb, service, Collections.emptyList());
    }
    
 // internal 
    protected InputMapper(final InputMapperConfiguration configuration,
            final JsonBuilderFactory jsonFactory,
            final Jsonb jsonb,
            final Hazelcast_componentService service,
            final Collection<String> members) {
        this.configuration = configuration;
        this.jsonFactory = jsonFactory;
        this.jsonb = jsonb;
        this.service = service;
        this.members = members;
    }
    
    @PostConstruct
    public void init() throws IOException {
        instance = service.findInstance(configuration.newConfig()); 
    }
    
    @PreDestroy
    public void close() { 
        instance.getLifecycleService().shutdown();
        executorService = null;
    }

    @Assessor
    public long estimateSize() {
        // this method should return the estimation of the dataset size
        // it is recommended to return a byte value
        // if you don't have the exact size you can use a rough estimation
//        return 1L;
    	return getSizeByMembers() 
                .values().stream()
                .mapToLong(this::getFutureValue)
                .sum();
    }

    private Map<Member, Future<Long>> getSizeByMembers() {
        final IExecutorService executorService = getExecutorService();
        final SerializableTask<Long> sizeComputation = new SerializableTask<Long>() {

            @Override
            public Long call() throws Exception {

                return instance.getMap(configuration.getMapName()).getLocalMapStats().getHeapCost();
            }
        };
        if (members.isEmpty()) { // == if no specific members defined, apply on all the cluster
            return executorService.submitToAllMembers(sizeComputation);
        }
        final Set<Member> members = instance.getCluster().getMembers().stream()
                .filter(m -> this.members.contains(m.getUuid()))
                .collect(Collectors.toSet());
        return executorService.submitToMembers(sizeComputation, members);
    }

    private IExecutorService getExecutorService() {
        return executorService == null ?
                executorService = instance.getExecutorService(configuration.getExecutorService()) :
                executorService;
    }
    

    private long getFutureValue(final Future<Long> future) {
        try {
            return future.get(configuration.getTimeout(), TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (final ExecutionException | TimeoutException e) {
            throw new IllegalArgumentException(e);
        }
    }

	@Split
    public List<InputMapper> split(@PartitionSize final long bundles) {
        // overall idea here is to split the work related to configuration in bundles of size "bundles"
        //
        // for instance if your estimateSize() returned 1000 and you can run on 10 nodes
        // then the environment can decide to run it concurrently (10 * 100).
        // In this case bundles = 100 and we must try to return 10 InputMapper with 1/10 of the overall work each.
        //
        // default implementation returns this which means it doesn't support the work to be split
//        return singletonList(this);
		final List<InputMapper> partitions = new ArrayList<>();
	    final Collection<Member> members = new ArrayList<>();
	    long current = 0;
	    for (final Map.Entry<Member, Future<Long>> entries : getSizeByMembers().entrySet()) {
	        final long memberSize = getFutureValue(entries.getValue());
	        if (members.isEmpty()) {
	            members.add(entries.getKey());
	            current += memberSize;
	        } else if (current + memberSize > bundles) {
	            partitions.add(
	                    new InputMapper(configuration, jsonFactory, jsonb, service, toIdentifiers(members)));
	            // reset current iteration
	            members.clear();
	            current = 0;
	        }
	    }
	    if (!members.isEmpty()) {
	        partitions.add(new InputMapper(configuration, jsonFactory, jsonb, service, toIdentifiers(members)));
	    }

	    if (partitions.isEmpty()) { // just execute this if no plan (= no distribution)
	        partitions.add(this);
	    }
	    return partitions;
    }


    private Set<String> toIdentifiers(final Collection<Member> members) {
        return members.stream().map(Member::getUuid).collect(Collectors.toSet());
    }

	@Emitter
    public InputSource createWorker() {
        // here we create an actual worker,
        // you are free to rework the configuration etc but our default generated implementation
        // propagates the partition mapper entries.
//        return new InputSource(configuration, service, jsonFactory);
        return new InputSource(configuration, jsonFactory, jsonb, service, members);
        
    }
}