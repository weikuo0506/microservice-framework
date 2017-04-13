package com.walker.microservice.framework.registry;

/**
 * Created by walker on 2017/4/13.
 */
public interface ServiceRegistry {

    void register(String serviceName, String serviceAddr);
}
