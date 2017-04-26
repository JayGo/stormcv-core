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

    private DriverManagerDataSource dataSource = null;
    private static final DBManager dbManager = new DBManager();
    private JdbcTemplate jdbcTemplate = null;

    /**
     * Get DBManager Instance
     */
    public static DBManager getInstance() {
        return dbManager;
    }

    /**
     * Initialize MySql
     */
    private DBManager() {
        initDataSource();
        this.jdbcTemplate = new JdbcTemplate(this.dataSource);
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

//    /**
//     * Create all necessary table
//     *
//     * @throws SQLException
//     */
//    private void initAllTables() throws SQLException {
//        createTableMap.put(CAMERA_INFO_TABLE, CREATE_CAMERA_INFO_TABLE);
//        createTableMap.put(RAW_RTMP_TABLE, CREATE_RAW_RTMP_TABLE);
//        createTableMap.put(EFFECT_RTMP_TABLE, CREATE_EFFECT_RTMP_TABLE);
//        for (String table : createTableMap.keySet()) {
//            if (!validateTableExist(table)) {
//                this.jdbcTemplate.execute(createTableMap.get(table));
//            }
//        }
//        logger.info("cerate all tables...");
//    }

//    private void dropAllTables() {
//        dropTable(CAMERA_INFO_TABLE);
//        dropTable(RAW_RTMP_TABLE);
//        dropTable(EFFECT_RTMP_TABLE);
//        logger.info("drop all Tables...");
//    }

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

    public JdbcTemplate geJdbcTemplate() {
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
            address = dbManager.geJdbcTemplate().queryForObject("SELECT rtmp_addr FROM " + DBManager.RAW_RTMP_TABLE + " WHERE stream_id = ?", new Object[] {streamId}, String.class);
        } catch (Exception e) {
            logger.error("cannot query rtmp_addr for streamId {} due to {}", streamId, e.getMessage());
        }
        return address;
    }

    public CameraInfo getCameraInfo(String streamId) {
        CameraInfo ret = new CameraInfo();

        Map<String, Object> queryRet = dbManager.geJdbcTemplate().queryForMap(
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
        Map<String, Object> rows = dbManager.geJdbcTemplate()
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
