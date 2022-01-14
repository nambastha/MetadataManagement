package com.metadata;

import com.metadata.manager.TraverseGraph;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

@SpringBootApplication
public class AtlasConnector {

    @Autowired
    TraverseGraph traverseGraph ;

    public static void main(String[] args){
        SpringApplication.run(AtlasConnector.class, args);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void getGraph(){
        traverseGraph.getGraphTraversal();
    }
}
