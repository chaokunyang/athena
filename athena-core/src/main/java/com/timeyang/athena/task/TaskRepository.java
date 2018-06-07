package com.timeyang.athena.task;

import com.timeyang.athena.AthenaException;
import com.timeyang.athena.task.TaskInfo.FinishedTask;
import com.timeyang.athena.task.TaskInfo.RunningTask;
import com.timeyang.athena.task.TaskInfo.WaitingTask;
import com.timeyang.athena.utill.jdbc.JdbcUtils;
import com.timeyang.athena.utill.jdbc.JdbcUtils.RowMapper;
import com.timeyang.athena.utill.jdbc.Page;
import com.timeyang.athena.utill.jdbc.PagedResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * access database to get all kinds of task
 *
 * @author https://github.com/chaokunyang
 */
public class TaskRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskRepository.class);
    public static final String WAITING_TASK_TABLE = "waiting_task";
    public static final String RUNNING_TASK_TABLE = "running_task";
    public static final String FINISHED_TASK_TABLE = "finished_task";

    private final DataSource dataSource;

    TaskRepository(DataSource dataSource) {
        this.dataSource = dataSource;
        createTaskTableIfAbsent();
    }

    private void createTaskTableIfAbsent() {
        String waitingTaskSql = "CREATE TABLE " + WAITING_TASK_TABLE +
                "(" +
                "task_id INTEGER GENERATED ALWAYS AS IDENTITY " +
                "(START WITH 10000000, INCREMENT BY 1), " +
                "task_name VARCHAR(100), " +
                "task_type VARCHAR(100), " +
                "host VARCHAR(100), " +
                "class_name VARCHAR(100), " +
                "classpath VARCHAR(30000), " +
                "params VARCHAR(10000), " +
                "max_tries INTEGER, " +
                "retry_wait BIGINT, " +
                "submit_time TIMESTAMP" +
                ")";
        String runningTaskSql = "CREATE TABLE " + RUNNING_TASK_TABLE +
                "(" +
                "task_id INTEGER PRIMARY KEY, " +
                "task_name VARCHAR(100), " +
                "task_type VARCHAR(100), " +
                "host VARCHAR(100), " +
                "class_name VARCHAR(100), " +
                "classpath VARCHAR(30000), " +
                "params VARCHAR(10000), " +
                "max_tries INTEGER, " +
                "try_number INTEGER, " +
                "retry_wait BIGINT, " +
                "submit_time TIMESTAMP, " +
                "start_time TIMESTAMP, " +
                "pid INTEGER" +
                ")";
        String finishedTaskSql = "CREATE TABLE " + FINISHED_TASK_TABLE +
                "(" +
                "task_id INTEGER PRIMARY KEY, " +
                "task_name VARCHAR(100), " +
                "task_type VARCHAR(100), " +
                "host VARCHAR(100), " +
                "class_name VARCHAR(100), " +
                "classpath VARCHAR(30000), " +
                "params VARCHAR(10000), " +
                "max_tries INTEGER, " +
                "try_number INTEGER, " +
                "retry_wait BIGINT, " +
                "submit_time TIMESTAMP, " +
                "start_time TIMESTAMP, " +
                "end_time TIMESTAMP, " +
                "duration BIGINT, " +
                "state VARCHAR(40)" +
                ")";

        try (Connection connection = this.dataSource.getConnection()) {
            boolean waitingTaskTableCreated = JdbcUtils.createTableIfAbsent(connection, WAITING_TASK_TABLE, waitingTaskSql);
            if (waitingTaskTableCreated) LOGGER.info("Created table " + WAITING_TASK_TABLE);

            boolean runningTaskTableCreated = JdbcUtils.createTableIfAbsent(connection, RUNNING_TASK_TABLE, runningTaskSql);
            if (runningTaskTableCreated) LOGGER.info("Created table " + RUNNING_TASK_TABLE);

            boolean finishedTaskTableCreated = JdbcUtils.createTableIfAbsent(connection, FINISHED_TASK_TABLE, finishedTaskSql);
            if (finishedTaskTableCreated) LOGGER.info("Created table " + FINISHED_TASK_TABLE);
        } catch (SQLException e) {
            throw new AthenaException("Can't get connection", e);
        }
    }


    //************************ Waiting task ************************
    public TaskInfo create(TaskInfo task) {
        String sql = "INSERT INTO " + WAITING_TASK_TABLE +
                "(task_name, host, class_name, params, max_tries, submit_time, classpath, retry_wait, task_type) " +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement pStatement = connection.prepareStatement(sql, new String[]{"TASK_ID"})) {
            pStatement.setString(1, task.getTaskName());
            pStatement.setString(2, task.getHost());
            pStatement.setString(3, task.getClassName());
            pStatement.setString(4, task.getParams());
            pStatement.setInt(5, task.getMaxTries());
            pStatement.setTimestamp(6, Timestamp.from(task.getSubmitTime()));
            pStatement.setString(7, task.getClasspath());
            pStatement.setLong(8, task.getRetryWait());
            pStatement.setString(9, task.getTaskType().toString());

            int affectedRows = pStatement.executeUpdate();
            if (affectedRows == 0) {
                throw new AthenaException("Creating waiting task failed, no rows affected.");
            }
            try (ResultSet generatedKeys = pStatement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    Long id = generatedKeys.getLong(1);
                    task.setTaskId(id);
                }
            }
        } catch (SQLException e) {
            String msg = String.format("Can't create waiting task [%s], sql: %s", task, sql);
            LOGGER.error(msg, e);
            throw new AthenaException(msg, e);
        }

        return task;
    }

    public WaitingTask getWaitingTask(long taskId) {
        String sql = String.format("select * from %s where task_id = %d",
                WAITING_TASK_TABLE, taskId);
        List<WaitingTask> query = JdbcUtils.query(dataSource, sql, waitingTaskRowMapper);
        if (query.isEmpty()) {
            return null;
        } else {
            return query.get(0);
        }
    }

    public PagedResult<WaitingTask> getWaitingTasks(Page page) {
       return JdbcUtils.queryPage(dataSource, WAITING_TASK_TABLE, page, waitingTaskRowMapper);
    }

    public List<WaitingTask> getAllWaitingTasks() {
        PagedResult<WaitingTask> pagedResult = getWaitingTasks(new Page(0, Integer.MAX_VALUE));
        return pagedResult.getElements();
    }

    public void deleteWaitingTask(Long taskId) {
        String sql = String.format("delete from %s WHERE task_id = %d", WAITING_TASK_TABLE, taskId);
        try (Connection connection = this.dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            String msg = String.format("Can't delete waiting task of task_id [%d], sql [%s]", taskId, sql);
            throw new AthenaException(msg, e);
        }
    }

    public void deleteAllWaitingTask() {
        try (Connection connection = this.dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            String sql = "DELETE FROM " + WAITING_TASK_TABLE;
            statement.execute(sql);
        } catch (SQLException e) {
            throw new AthenaException("Can't delete all waiting tasks", e);
        }
    }


    //************************ Running task ************************
    public void moveToRunning(RunningTask task) {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = this.dataSource.getConnection();
            connection.setAutoCommit(false);
            statement = connection.createStatement();

            long taskId = task.getTaskId();
            String deleteWaitingTaskSql = String.format("delete from %s WHERE task_id = %d", WAITING_TASK_TABLE, taskId);
            statement.execute(deleteWaitingTaskSql);

            String createRunningTaskSql = "INSERT INTO " + RUNNING_TASK_TABLE +
                    "(task_id, task_name, host, class_name, params, max_tries, submit_time, start_time, try_number, pid, classpath, retry_wait, task_type) " +
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement pStatement = connection.prepareStatement(createRunningTaskSql);
            pStatement.setLong(1, task.getTaskId());
            pStatement.setString(2, task.getTaskName());
            pStatement.setString(3, task.getHost());
            pStatement.setString(4, task.getClassName());
            pStatement.setString(5, task.getParams());
            pStatement.setInt(6, task.getMaxTries());
            pStatement.setTimestamp(7, Timestamp.from(task.getSubmitTime()));
            pStatement.setTimestamp(8, Timestamp.from(task.getStartTime()));
            pStatement.setInt(9, task.getTryNumber());
            pStatement.setInt(10, task.getPid());
            pStatement.setString(11, task.getClasspath());
            pStatement.setLong(12, task.getRetryWait());
            pStatement.setString(13, task.getTaskType().toString());

            pStatement.execute();

            connection.commit();
        } catch (SQLException e) {
            LOGGER.error("Move waiting task to running failed. task: " + task, e);
        } finally {
            close(connection, statement);
        }
    }

    public void updateRunningTask(RunningTask task) {
        String sql = "UPDATE " + RUNNING_TASK_TABLE + " " +
                "SET try_number = ?, pid = ? " +
                "WHERE task_id = ?";
        try (Connection connection = this.dataSource.getConnection();
             PreparedStatement pStatement = connection.prepareStatement(sql, new String[]{"TASK_ID"})) {
            pStatement.setInt(1, task.getTryNumber());
            pStatement.setInt(2, task.getPid());
            pStatement.setLong(3, task.getTaskId());

            pStatement.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new AthenaException("Can't create waiting task, sql: " + sql, e);
        }
    }

    public Optional<RunningTask> getRunningTask(long taskId) {
        String sql = String.format("select * from %s where task_id = %d",
                RUNNING_TASK_TABLE, taskId);
        List<RunningTask> query = JdbcUtils.query(dataSource, sql, runningTaskRowMapper);
        if (query.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(query.get(0));
        }
    }

    public List<RunningTask> getAllRunningTasks() {
        Page page = new Page(0, Integer.MAX_VALUE);
        return getRunningTasks(page).getElements();
    }

    public PagedResult<RunningTask> getRunningTasks(Page page) {
        return JdbcUtils.queryPage(dataSource, RUNNING_TASK_TABLE, page, runningTaskRowMapper);
    }


    //************************ Finished Task ************************
    public void moveFromWaitingToFinished(FinishedTask task) {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = this.dataSource.getConnection();
            connection.setAutoCommit(false);
            statement = connection.createStatement();

            long taskId = task.getTaskId();
            String deleteRunningTaskSql = String.format("delete from %s WHERE task_id = %d", WAITING_TASK_TABLE, taskId);
            statement.execute(deleteRunningTaskSql);

            String createFinishedTaskSql = "INSERT INTO " + FINISHED_TASK_TABLE +
                    "(task_id, task_name, host, class_name, params, max_tries,submit_time,  state, try_number, classpath, retry_wait, task_type) " +
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement pStatement = connection.prepareStatement(createFinishedTaskSql);
            pStatement.setLong(1, task.getTaskId());
            pStatement.setString(2, task.getTaskName());
            pStatement.setString(3, task.getHost());
            pStatement.setString(4, task.getClassName());
            pStatement.setString(5, task.getParams());
            pStatement.setInt(6, task.getMaxTries());
            pStatement.setTimestamp(7, Timestamp.from(task.getSubmitTime()));
            pStatement.setString(8, task.getState().toString());
            pStatement.setInt(9, task.getTryNumber());
            pStatement.setString(10, task.getClasspath());
            pStatement.setLong(11, task.getRetryWait());
            pStatement.setString(12, task.getTaskType().toString());

            pStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            LOGGER.error("Move waiting task to finished failed. task: " + task, e);
        } finally {
            close(connection, statement);
        }
    }

    public void moveToFinished(FinishedTask task) {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = this.dataSource.getConnection();
            connection.setAutoCommit(false);
            statement = connection.createStatement();

            long taskId = task.getTaskId();
            String deleteRunningTaskSql = String.format("delete from %s WHERE task_id = %d", RUNNING_TASK_TABLE, taskId);
            statement.execute(deleteRunningTaskSql);

            String createFinishedTaskSql = "INSERT INTO " + FINISHED_TASK_TABLE +
                    "(task_id, task_name, host, class_name, params, max_tries, submit_time, start_time, end_time, duration, state, try_number, classpath, retry_wait, task_type) " +
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement pStatement = connection.prepareStatement(createFinishedTaskSql);
            pStatement.setLong(1, task.getTaskId());
            pStatement.setString(2, task.getTaskName());
            pStatement.setString(3, task.getHost());
            pStatement.setString(4, task.getClassName());
            pStatement.setString(5, task.getParams());
            pStatement.setInt(6, task.getMaxTries());
            pStatement.setTimestamp(7, Timestamp.from(task.getSubmitTime()));
            pStatement.setTimestamp(8, Timestamp.from(task.getStartTime()));
            pStatement.setTimestamp(9, Timestamp.from(task.getEndTime()));
            pStatement.setLong(10, task.getDuration().getSeconds());
            pStatement.setString(11, task.getState().toString());
            pStatement.setInt(12, task.getTryNumber());
            pStatement.setString(13, task.getClasspath());
            pStatement.setLong(14, task.getRetryWait());
            pStatement.setString(15, task.getTaskType().toString());

            pStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            LOGGER.error("Move running task to finished failed. task: " + task, e);
        } finally {
            close(connection, statement);
        }
    }

    private void close(Connection connection, Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }

        if (connection != null) {
            try {
                connection.rollback();
                connection.close();
            } catch (SQLException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    public List<FinishedTask> getAllFinishedTasks() {
        Page page = new Page(0, Integer.MAX_VALUE);
        return getFinishedTasks(page).getElements();
    }

    public PagedResult<FinishedTask> getFinishedTasks(Page page) {
        return JdbcUtils.queryPage(dataSource, FINISHED_TASK_TABLE, page, finishedTaskRowMapper);
    }

    public FinishedTask getFinishedTask(long taskId) {
        String sql = String.format("select * from %s where task_id=%d",
                FINISHED_TASK_TABLE, taskId);

        List<FinishedTask> tasks = JdbcUtils.query(
                this.dataSource, sql, finishedTaskRowMapper);

        if (!tasks.isEmpty())
            return tasks.get(0);
        else
            return null;
    }

    private RowMapper<WaitingTask> waitingTaskRowMapper = (rs, rowNum) -> {
        WaitingTask task = new WaitingTask();
        task.setTaskId(rs.getLong("task_id"));
        task.setTaskName(rs.getString("task_name"));
        task.setTaskType(TaskType.valueOf(rs.getString("task_type")));
        task.setHost(rs.getString("host"));
        task.setClassName(rs.getString("class_name"));
        task.setClasspath(rs.getString("classpath"));
        task.setParams(rs.getString("params"));
        task.setMaxTries(rs.getInt("max_tries"));
        task.setRetryWait(rs.getLong("retry_wait"));
        task.setSubmitTime(rs.getTimestamp("submit_time").toInstant());

        return task;
    };

    private RowMapper<RunningTask> runningTaskRowMapper = (rs, rowNum) -> {
        RunningTask task = new RunningTask();
        task.setTaskId(rs.getLong("task_id"));
        task.setTaskName(rs.getString("task_name"));
        task.setTaskType(TaskType.valueOf(rs.getString("task_type")));
        task.setHost(rs.getString("host"));
        task.setClassName(rs.getString("class_name"));
        task.setClasspath(rs.getString("classpath"));
        task.setParams(rs.getString("params"));
        task.setMaxTries(rs.getInt("max_tries"));
        task.setRetryWait(rs.getLong("retry_wait"));
        task.setSubmitTime(rs.getTimestamp("submit_time").toInstant());
        task.setStartTime(rs.getTimestamp("start_time").toInstant());
        task.setTryNumber(rs.getInt("try_number"));
        task.setPid(rs.getInt("pid"));

        return task;
    };

    private RowMapper<FinishedTask> finishedTaskRowMapper = (rs, rowNum) -> {
        FinishedTask task = new FinishedTask();
        task.setTaskId(rs.getLong("task_id"));
        task.setTaskName(rs.getString("task_name"));
        task.setTaskType(TaskType.valueOf(rs.getString("task_type")));
        task.setHost(rs.getString("host"));
        task.setClassName(rs.getString("class_name"));
        task.setClasspath(rs.getString("classpath"));
        task.setParams(rs.getString("params"));
        task.setMaxTries(rs.getInt("max_tries"));
        task.setRetryWait(rs.getLong("retry_wait"));
        task.setSubmitTime(rs.getTimestamp("submit_time").toInstant());

        Timestamp startTime = rs.getTimestamp("start_time");
        if (startTime != null)
            task.setStartTime(startTime.toInstant());
        task.setDuration(Duration.ofSeconds(rs.getLong("duration")));
        Timestamp endTime = rs.getTimestamp("end_time");
        if (endTime != null) {
            task.setEndTime(endTime.toInstant());
        }
        task.setState(TaskState.valueOf(rs.getString("state")));
        task.setTryNumber(rs.getInt("try_number"));

        return task;
    };

}
