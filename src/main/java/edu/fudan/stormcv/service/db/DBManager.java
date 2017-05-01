package edu.fudan.stormcv.service.db;

import edu.fudan.stormcv.service.model.CameraInfo;
import edu.fudan.stormcv.service.model.EffectRtmpInfo;
import edu.fudan.stormcv.util.ConfigUtil;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 4/24/17 - 2:51 PM
 * Description:
 */
public class DBManager {
    private static final Logger logger = LoggerFactory.getLogger(DBManager.class);

    public static final String CAMERA_INFO_TABLE = "camera_info";
    public static final String CAMERA_INFO_TABLE_SIMPLE_COLS = "(stream_id, address)";
    public static final String CAMERA_INFO_TABLE_COLS = "(stream_id, address, valid, width, height, frame_rate)";

    public static final String RAW_RTMP_TABLE = "raw_rtmp";
    public static final String RAW_RTMP_TABLE_COLS = "(stream_id, rtmp_addr, host, pid, valid)";

    public static final String EFFECT_RTMP_TABLE = "effect_rtmp";
    public static final String EFFECT_RTMP_TABLE_COLS = "(stream_id, effect_type, effect_params, rtmp_addr, topo_id, valid)";

    public static final String TOPOLOGY_TABLE = "topology";
    public static final String CREATE_TOPOLOGY_TABLE = "CREATE TABLE topology(" +
            "topo_name varchar(255) NOT NULL," +
            "topo_id varchar(255) NOT NULL, " +
            "worker_num int NOT NULL DEFAULT 0, " +
            "owner varchar(255) NOT NULL, " +
            "uptime_secs int NOT NULL DEFAULT 0, " +
            "task_num int NOT NULL DEFAULT 0, " +
            "executor_num int NOT NULL DEFAULT 0, " +
            "status int NOT NULL DEFAULT 0, " +
            "PRIMARY KEY(topo_name));";
    public static final String TOPOLOGY_TABLE_COLS = "(topo_name, topo_id, worker_num, owner, uptime_secs, task_num, " +
            "executor_num, status)";


    public static final String TOPOLOGY_COMPONENT_INFO_TABLE = "topology_component_info";
    public static final String CREATE_TOPOLOGY_COMPONENT_INFO_TABLE = "CREATE TABLE topology_component_info(" +
            "topo_name varchar(255) NOT NULL, " +
            "component_id varchar(255) NOT NULL, " +
            "type varchar(255) NOT NULL, " +
            "executor_num int NOT NULL DEFAULT 0, " +
            "task_num int NOT NULL DEFAULT 0, " +
            "alltime_processed bigint NOT NULL DEFAULT 0, " +
            "alltime_failed bigint NOT NULL DEFAULT 0, " +
            "alltime_latency double NOT NULL DEFAULT 0.0, " +
            "PRIMARY KEY(topo_name, component_id));";
    public static final String TOPOLOGY_COMPONENT_INFO_COLS = "(topo_name, component_id, type, executor_num, " +
            "task_num, alltime_processed, alltime_failed, alltime_latency)";


    public static final String TOPOLOGY_WORKER_INFO_TABLE = "topology_worker_info";
    public static final String CREATE_TOPOLOGY_WORKER_INFO_TABLE = "CREATE TABLE topology_worker_info(" +
            "topo_name varchar(255) NOT NULL, " +
            "host varchar(255) NOT NULL, " +
            "pid bigint NOT NULL DEFAULT 0, " +
            "port int NOT NULL DEFAULT 0, " +
            "cpu_usage double NOT NULL DEFAULT 0.0, " +
            "memory_usage double NOT NULL DEFAULT 0.0, " +
            "PRIMARY KEY(topo_name, host, port));";
    public static final String TOPOLOGY_WORKER_INFO_TABLE_COLS = "(topo_name, host, pid, port, cpu_usage, memory_usage)";


    private DriverManagerDataSource dataSource = null;
    private static DBManager dbManager = null;
    private JdbcTemplate jdbcTemplate = null;

    private Map<String, String> createTableMap = new HashMap<>();

    /**
     * Get DBManager Instance
     */
    public static synchronized DBManager getInstance() {
        if (dbManager == null) {
            dbManager = new DBManager();
        }
        return dbManager;
    }

    /**
     * Initialize MySql
     */
    private DBManager() {
        initDataSource();
        this.jdbcTemplate = new JdbcTemplate(this.dataSource);
        dropAllTables();
        try {
            initAllTables();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        DBManager.getInstance();
    }

    /**
     * initiate the data source
     */
    private void initDataSource() {
        dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        String dbUrl = ConfigUtil.getDBIp();
        String username = ConfigUtil.getDBUser();
        String password = ConfigUtil.getDBPassword();
        String dbName = ConfigUtil.getDBName();
        int port = ConfigUtil.getDBPort();
        String mysqlUrl = "jdbc:mysql://" + dbUrl + ":" + port + "/" + dbName + "?autoReconnect=true";
        System.out.println(mysqlUrl);
        dataSource.setUrl(mysqlUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
    }

    /**
     * Create all necessary table
     *
     * @throws SQLException
     */
    private void initAllTables() throws SQLException {
        createTableMap.put(TOPOLOGY_TABLE, CREATE_TOPOLOGY_TABLE);
        createTableMap.put(TOPOLOGY_WORKER_INFO_TABLE, CREATE_TOPOLOGY_WORKER_INFO_TABLE);
        createTableMap.put(TOPOLOGY_COMPONENT_INFO_TABLE, CREATE_TOPOLOGY_COMPONENT_INFO_TABLE);
        for (String table : createTableMap.keySet()) {
            if (!validateTableExist(table)) {
                this.jdbcTemplate.execute(createTableMap.get(table));
            }
        }
        logger.info("cerate all tables...");
    }

    private void dropAllTables() {
        dropTable(TOPOLOGY_TABLE);
        dropTable(TOPOLOGY_COMPONENT_INFO_TABLE);
        dropTable(TOPOLOGY_WORKER_INFO_TABLE);
        logger.info("drop all Tables...");
    }

    /**
     * validate if the table exist
     *
     * @param tableName
     * @return
     */
    public boolean validateTableExist(String tableName) {
        ResultSet rs;
        boolean flag = false;
        try {
            DatabaseMetaData meta = dataSource.getConnection().getMetaData();
            String type[] = { "TABLE" };
            rs = meta.getTables(null, null, tableName, type);
            flag = rs.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return flag;
    }

    /**
     * Get DriverManagerDatasource Instance
     */
    public DriverManagerDataSource getDataSource() {
        return dataSource;
    }

    public JdbcTemplate getJdbcTemplate() {
        return this.jdbcTemplate;
    }

    public void dropTable(String tableName) {
        String query = "DROP TABLE " + tableName + ";";
        try {
            this.jdbcTemplate.execute(query);
        } catch (Exception e) {
            logger.warn("drop table {} failed due to {}", tableName, e.getMessage());
        }
    }

    public void clearAllRecords(String tableName) {
        String query = "DELETE FROM " + tableName + ";";
        this.jdbcTemplate.execute(query);
    }

    public String getCameraRawRtmpAddress(String streamId) {
        String address = null;
        try {
            address = dbManager.getJdbcTemplate().queryForObject("SELECT rtmp_addr FROM " + DBManager.RAW_RTMP_TABLE + " WHERE stream_id = ?", new Object[] {streamId}, String.class);
        } catch (Exception e) {
            logger.error("cannot query rtmp_addr for streamId {} due to {}", streamId, e.getMessage());
        }
        return address;
    }

    public CameraInfo getCameraInfo(String streamId) {
        CameraInfo ret = new CameraInfo();

        Map<String, Object> queryRet = dbManager.getJdbcTemplate().queryForMap(
                "SELECT * FROM " + DBManager.CAMERA_INFO_TABLE + " WHERE stream_id = ?", new Object[] { streamId });
        ret.setStreamId((String) queryRet.get("stream_id"));
        ret.setAddress((String) queryRet.get("address"));
        ret.setValid(((int) queryRet.get("valid") == 0) ? true : false);
        ret.setWidth((int) queryRet.get("width"));
        ret.setHeight((int) queryRet.get("height"));
        ret.setFrameRate((float) queryRet.get("frame_rate"));
        logger.info(ret.toString());

        return ret;
    }


    public EffectRtmpInfo getCameraEffectRtmpInfo(int id) {
        EffectRtmpInfo ret = new EffectRtmpInfo();
        Map<String, Object> rows = dbManager.getJdbcTemplate()
                .queryForMap("SELECT * FROM " + DBManager.EFFECT_RTMP_TABLE + " WHERE id = ?", new Object[] { id });
        ret.setId(id);
        ret.setStreamId((String) rows.get("stream_id"));
        ret.setRtmpAddress((String) rows.get("rtmp_addr"));
        ret.setValid(((int) rows.get("valid") == 0) ? true : false);
        ret.setTopoName((String) rows.get("topo_name"));
        ret.setEffectType((String) rows.get("effect_type"));
        String paramStr = (String) rows.get("effect_params");

        ret.setEffectParams(new JSONObject(paramStr).toMap());
        logger.info("[{}]{}", "getCameraRawRtmpInfo", ret.toString());
        return ret;
    }

}
