package io.github.jas34.scheduledwf;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.orchestration.ExecutionDAOFacade;
import com.netflix.conductor.dao.*;
import com.netflix.conductor.mysql.dao.MySQLExecutionDAO;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
@Primary
@ConditionalOnProperty(name = "conductor.db.type", havingValue = "mysql")
public class ExecutionDAOFacadeExt extends ExecutionDAOFacade {

    private final MySQLExecutionDAOExt executionDAO;


    public ExecutionDAOFacadeExt(ExecutionDAO executionDAO, QueueDAO queueDAO, IndexDAO indexDAO, RateLimitingDAO rateLimitingDao, PollDataDAO pollDataDAO, ObjectMapper objectMapper, ConductorProperties properties,
                                 DataSource dataSource) {
        super(executionDAO, queueDAO, indexDAO, rateLimitingDao, pollDataDAO, objectMapper, properties);
        this.executionDAO = new MySQLExecutionDAOExt(objectMapper, dataSource);
    }

    /**
     * ALTER TABLE workflow
     * ADD FULLTEXT INDEX ft_index_json_data (json_data);
     * <p>
     * SELECT *
     * FROM workflow
     * WHERE MATCH(json_data) AGAINST('your_search_term' IN NATURAL LANGUAGE MODE);
     *
     * @param query
     * @param freeText
     * @param start
     * @param count
     * @param sort
     * @return
     */
    @Override
    public SearchResult<String> searchWorkflows(String query, String freeText, int start, int count, List<String> sort) {
        String GET_TASK = "SELECT json_data FROM task WHERE task_id = ?";
        SearchResult<String> result = new SearchResult<>();
        StringBuilder sql = new StringBuilder("SELECT * FROM workflow WHERE ");
        boolean requiresAnd = false;
        if(!StringUtils.isEmpty(freeText) && freeText.length()>1){
            requiresAnd = true;
            sql.append("(MATCH(json_data) AGAINST('").append(freeText
                            .replaceAll("\"","")
                    .replaceAll("'", "''")).append("' IN NATURAL LANGUAGE MODE))");
        }
        if(!StringUtils.isEmpty(query)){
            Map<String, String> queries = parseQueryString(query);
            if(queries.containsKey("workflowId")){
                sql.append((requiresAnd ? " AND ":"") + "workflow_id = '"+queries.get("workflowId")+"' ");
                requiresAnd = true;
            }
            if(queries.containsKey("workflowType_IN")){
                sql.append("(MATCH(json_data) AGAINST('").append(queries.get("workflowType_IN")
                        .replaceAll("\"","")
                        .replaceAll("'", "''")).append("' IN NATURAL LANGUAGE MODE))");
                requiresAnd = true;
            }
            if(queries.containsKey("startTime>")){
                Long to = Long.parseLong(queries.get("startTime>"))/1000;
                sql.append((requiresAnd ? " AND ":"") + "modified_on > FROM_UNIXTIME("+to+") ");
                requiresAnd = true;
            }
            if(queries.containsKey("startTime<")){
                Long to = Long.parseLong(queries.get("startTime<"))/1000;
                sql.append((requiresAnd ? " AND ":"") + "modified_on < FROM_UNIXTIME("+to+") ");
                requiresAnd = true;
            }
        }
        if(!CollectionUtils.isEmpty(sort)){
            String fistSort = sort.get(0);
            if(fistSort.contains("DESC")){
                sql.append(" ORDER BY modified_on DESC");
            }else{
                sql.append(" ORDER BY modified_on ASC");
            }
        }
        sql.append(" LIMIT ").append(count);
        sql.append(" OFFSET ").append(start);

         String SQL_QUERY = sql.toString();
        if(!requiresAnd){
            SQL_QUERY = SQL_QUERY.replace("WHERE"," ");
        }
         try (Connection connection = executionDAO.ds.getConnection();
             PreparedStatement statement = connection.prepareStatement(SQL_QUERY)) {
            ResultSet resultSet = statement.executeQuery();
            List<String > wfids = new ArrayList<>();
            while (resultSet.next()){
                wfids.add(resultSet.getString("workflow_id"));
            }
            result.setResults(wfids);
            result.setTotalHits(wfids.size());
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return result;
    }
    public static Map<String, String> parseQueryString(String queryString) {
        Map<String, String> queryMap = new HashMap<>();

        // Define patterns for key-value pairs and IN conditions
        Pattern keyValuePattern = Pattern.compile("(\\w+)\\s*([<>!=]+)\\s*\"?([^\"\\s]+)\"?");
        Pattern inConditionPattern = Pattern.compile("(\\w+)\\s+IN\\s+\\(([^)]+)\\)");

        Matcher keyValueMatcher = keyValuePattern.matcher(queryString);
        Matcher inConditionMatcher = inConditionPattern.matcher(queryString);

        // Match key-value pairs
        while (keyValueMatcher.find()) {
            String key = keyValueMatcher.group(1);
            String operator = keyValueMatcher.group(2);
            String value = keyValueMatcher.group(3);

            // Add key-value pairs to the map
            queryMap.put(key + operator, value);
        }

        // Match IN conditions
        while (inConditionMatcher.find()) {
            String key = inConditionMatcher.group(1);
            String values = inConditionMatcher.group(2);

            // Add IN conditions to the map
            queryMap.put(key + "_IN", values);
        }

        return queryMap;
    }
    /**
     * ALTER TABLE task
     * ADD FULLTEXT INDEX ft_index_json_data (json_data);
     * <p>
     * SELECT *
     * FROM task
     * WHERE MATCH(json_data) AGAINST('your_search_term' IN NATURAL LANGUAGE MODE);
     *
     * @param query
     * @param freeText
     * @param start
     * @param count
     * @param sort
     * @return
     */
    @Override
    public SearchResult<String> searchTasks(String query, String freeText, int start, int count, List<String> sort) {
        return super.searchTasks(query, freeText, start, count, sort);
    }
}
