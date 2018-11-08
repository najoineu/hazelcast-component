package com.company.talend.components.source;

import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;




@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
    @GridLayout.Row({ "hazelcastXml", "mapName" }),
    @GridLayout.Row({ "executorService" })
})
@Documentation("TODO fill the documentation for this configuration")
public class InputMapperConfiguration implements Serializable {
    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String hazelcastXml;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String mapName;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String executorService = "default";

    ClientConfig newConfig() throws IOException { 
        final ClientConfig newconfig = hazelcastXml == null ? new XmlClientConfigBuilder().build() :
                new XmlClientConfigBuilder(hazelcastXml).build();

        newconfig.setInstanceName(getClass().getSimpleName() + "_" + UUID.randomUUID().toString());
        newconfig.setClassLoader(Thread.currentThread().getContextClassLoader());
        return newconfig;
    }
    
    public String getHazelcastXml() {
        return hazelcastXml;
    }

    public InputMapperConfiguration setHazelcastXml(String hazelcastXml) {
        this.hazelcastXml = hazelcastXml;
        return this;
    }

    public String getMapName() {
        return mapName;
    }

    public InputMapperConfiguration setMapName(String mapName) {
        this.mapName = mapName;
        return this;
    }

    public String getExecutorService() {
        return executorService;
    }

    public InputMapperConfiguration setExecutorService(String executorService) {
        this.executorService = executorService;
        return this;
    }

	public long getTimeout() {
		// TODO Auto-generated method stub
		return 100;
	}
}