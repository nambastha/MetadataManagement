package com.metadata.manager;

import com.metadata.pojo.DbTablePOJO;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@RestController
public class TraverseGraph {
    private static final Logger logger = Logger.getLogger(TraverseGraph.class);

    public GraphTraversalSource g = null;
    public JanusGraph graph = null;


    public void getGraphTraversal(){
        graph = JanusGraphFactory.open("conf/atlas-hbase-solr.properties");
        g = graph.traversal();
    }


    private Map<String, Object> deserializeVertexProperties(Map<Object, Object> map) {
        HashMap<String, Object> deserializedProperties = new HashMap<>();
        map.forEach((key, value) -> {
            if (value instanceof List) {
                if (((List) value).size() > 1) {
                    logger.warn("Warning: value size > 1");
                }
                deserializedProperties.put((String) key, ((List) value).get(0));
            }
        });
        return deserializedProperties;
    }


    @PostMapping(value = "/getMetadataDetails")
    public List<Map<String, Object>> getMetadataDetails(@RequestBody DbTablePOJO dbTable) throws JsonProcessingException {
        String dbName = dbTable.getDbName();
        String tableName = dbTable.getTableName();
        List<Map<Object, Object>> graphConfigurations = null;
        List<Map<String, Object>> list = new ArrayList<>();

        if (Objects.isNull(g)) {
            getGraphTraversal();
        }
        if (g.V().count().next() == 0) {
            logger.info("empty graph, building DM");
        } else {
            logger.info("connection successful");
            graphConfigurations = g.V().has("Referenceable.__u_qualifiedName", dbName+"@mysql_db").in().valueMap().toList();
            for (Map<Object,Object> map : graphConfigurations){
                logger.info("----map----"+map);
                for (Map.Entry<Object,Object> entry : map.entrySet()) {
                    System.out.println(entry.getKey() + "/" + entry.getValue());
                    if (entry.getValue().toString().equalsIgnoreCase("["+tableName+"@mysql_table]")){
                        logger.info("Oh Yeah!");
                        graphConfigurations = g.V().has("Referenceable.__u_qualifiedName", tableName+"@mysql_table").out().valueMap().toList();
                        return  graphConfigurations.stream().map(this::deserializeVertexProperties).collect(Collectors.toList());
                    }
                }
            }
        }
        return list;
    }
    @PostMapping(value = "/getDefaultMetadataDetails")
    public List<Map<String, Object>> getDefaultMetadataDetails(@RequestBody DbTablePOJO dbTable) throws JsonProcessingException {
        String dbName = dbTable.getDbName();
        String tableName = dbTable.getTableName();
        List<Map<Object, Object>> graphConfigurations = null;
        List<Map<String, Object>> list = new ArrayList<>();

        if (Objects.isNull(g)) {
            getGraphTraversal();
        }
        if (g.V().count().next() == 0) {
            logger.info("empty graph, building DM");
        } else {
            logger.info("connection successful");
            graphConfigurations = g.V().has("Referenceable.__u_qualifiedName", dbName+"@cl1").in().valueMap().toList();
            for (Map<Object,Object> map : graphConfigurations){
                logger.info("----map----"+map);
                for (Map.Entry<Object,Object> entry : map.entrySet()) {
                    System.out.println(entry.getKey() + "/" + entry.getValue());
                    if (entry.getValue().toString().equalsIgnoreCase("["+tableName+"@cl1]")){
                        logger.info("Oh Yeah!");
                        graphConfigurations = g.V().has("Referenceable.__u_qualifiedName", tableName+"@cl1").out().valueMap().toList();
                        return  graphConfigurations.stream().map(this::deserializeVertexProperties).collect(Collectors.toList());
                    }
                }
            }
        }
        return list;
    }

    @RequestMapping(value = "/getVerticesCount/{dbName}", method = RequestMethod.GET)
    public List<Map<String, Object>> getVerticesCount(@PathVariable String dbName) throws JsonProcessingException {
        List<Map<Object, Object>> graphConfigurations = null;

        if (Objects.isNull(g)) {
            getGraphTraversal();
        }
        if (g.V().count().next() == 0) {
            logger.info("empty graph, building DM");
        } else {
            logger.info("connection successful");
            graphConfigurations = g.V().has("Referenceable.__u_qualifiedName", dbName+"@mysql_db").in().out().valueMap().toList();
        }
        List<Map<String, Object>> list = new ArrayList<>();
        for (Map<Object, Object> graphConfiguration : graphConfigurations) {
            Map<String, Object> stringObjectMap = deserializeVertexProperties(graphConfiguration);
            list.add(stringObjectMap);
        }
        return list;
    }

    @RequestMapping(value = "/getTableList/{dbName}", method = RequestMethod.GET)
    public List<Map<String, Object>> getTableList(@PathVariable String dbName) throws JsonProcessingException {
        List<Map<Object, Object>> graphConfigurations = null;

        if (Objects.isNull(g)) {
            getGraphTraversal();
        }
        if (g.V().count().next() == 0) {
            logger.info("empty graph, building DM");
        } else {
            logger.info("connection successful");
            graphConfigurations = g.V().has("Referenceable.__u_qualifiedName", dbName+"@mysql_db").in().valueMap().toList();
        }
        return  graphConfigurations.stream().map(this::deserializeVertexProperties).collect(Collectors.toList());
    }


    @RequestMapping(value = "/getColumnList/{tableName}", method = RequestMethod.GET)
    public List<Map<String, Object>> getColumnList(@PathVariable String tableName) throws JsonProcessingException {

        List<Map<Object, Object>> graphConfigurations = null;

        if (Objects.isNull(g)) {
            getGraphTraversal();
        }
        if (g.V().count().next() == 0) {
            logger.info("empty graph, building DM");
        } else {
            logger.info("connection successful");
            graphConfigurations = g.V().has("Referenceable.__u_qualifiedName", tableName+"@mysql_table").out().valueMap().toList();
        }
        return  graphConfigurations.stream().map(this::deserializeVertexProperties).collect(Collectors.toList());
    }

    @RequestMapping(value = "/getMetadata/{input}", method = RequestMethod.GET)
    public List<Map<String, Object>> getMetadata(@PathVariable String input) throws JsonProcessingException {
        String[] dbTableName = input.split("-");
        String dbName = dbTableName[0];
        String tableName = dbTableName[1];
        List<Map<Object, Object>> graphConfigurations = null;

        if (Objects.isNull(g)) {
            getGraphTraversal();
        }
        if (g.V().count().next() == 0) {
            logger.info("empty graph, building DM");
        } else {
            logger.info("connection successful");
            graphConfigurations = g.V().has("Referenceable.__u_qualifiedName", dbName+"@mysql_db").in().valueMap().toList();
            for (Map<Object,Object> map : graphConfigurations){
                logger.info("----map----"+map);
                for (Map.Entry<Object,Object> entry : map.entrySet()) {
                    System.out.println(entry.getKey() + "/" + entry.getValue());
                    if (entry.getValue().toString().equalsIgnoreCase("[sales@mysql_table]")){
                        logger.info("Oh Yeah!");
                        graphConfigurations = g.V().has("Referenceable.__u_qualifiedName", tableName+"@mysql_table").out().valueMap().toList();
                        return  graphConfigurations.stream().map(this::deserializeVertexProperties).collect(Collectors.toList());
                    }
                }
            }
        }
        return  graphConfigurations.stream().map(this::deserializeVertexProperties).collect(Collectors.toList());
    }

}

