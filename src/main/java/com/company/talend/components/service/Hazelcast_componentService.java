package com.company.talend.components.service;

import org.talend.sdk.component.api.service.Service;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

@Service
public class Hazelcast_componentService {

	public HazelcastInstance findInstance(final ClientConfig config) {
		return HazelcastClient.newHazelcastClient(config);
	}

    // you can put logic here you can reuse in components

}