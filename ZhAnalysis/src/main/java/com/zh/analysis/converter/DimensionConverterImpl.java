package com.zh.analysis.converter;

import com.zh.analysis.base.BaseDimension;
import com.zh.analysis.bean.ContactDimension;
import com.zh.analysis.bean.DateDimension;
import com.zh.analysis.util.JDBCInstance;
import com.zh.analysis.util.JDBCUtil;
import com.zh.analysis.util.LRUCache;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Author zhanghe
 *
 * 1、根据传入的维度数据，得到该数据对应的在表中的主键id
 * ** 做内存缓存，LRUCache
 * 分支
 * -- 缓存中有数据 -> 直接返回id
 * -- 缓存中无数据 ->
 * ** 查询Mysql
 * 分支：
 * -- Mysql中有该条数据 -> 直接返回id -> 将本次读取到的id缓存到内存中
 * -- Mysql中没有该数据  -> 插入该条数据 -> 再次反查该数据，得到id并返回 -> 缓存到内存中
 */
@Slf4j
public class DimensionConverterImpl implements DimensionConverter {

    // 管理JDBC线程的连接器
    private ThreadLocal<Connection> threadLocalConnection = new ThreadLocal<>();
    // 构建内存缓存对象
    private LRUCache cache = new LRUCache(3000);

    public DimensionConverterImpl() {
        //jvm关闭时，释放资源
        // getRuntime获取运行线程, 使用JDBC关闭
        Runtime.getRuntime().addShutdownHook(new Thread(() -> JDBCUtil.close(threadLocalConnection.get(), null, null)));
    }

    @Override
    public int getDimensionID(BaseDimension dimension) {
        //1、根据传入的维度对象获取对应的主键id，先从LRUCache中获取
        //时间维度：date_dimension_year_month_day, 10
        //联系人维度：contact_dimension_telephone_name(到了电话号码就不会重复了), 12
        String cacheKey = genCacheKey(dimension);
        //缓存里有，直接返回
        if (cache.containsKey(cacheKey)) {
            return cache.get(cacheKey);
        }

        //缓存里没有，执行select操作，sqls包含1组SQL操作：查询和插入
        String[] sql = null;
        String[] sqls = null;
        if (dimension instanceof DateDimension) {
            sqls = getDateDimensionSQL();  //时间维度的SQL
        } else if (dimension instanceof ContactDimension) {
            sqls = getContactDimensionSQL(); //联系人维度的SQL
        } else {
            throw new RuntimeException("没有匹配到对应维度信息.");
        }

        //准备对Mysql表进行操作，先查询，有可能再插入
        Connection conn = this.getConnection();
        int id = -1;
        synchronized (this) {
            id = execSQL(conn, sqls, dimension);
        }
        //将刚查询到的id加入到缓存中
        cache.put(cacheKey, id);
        return id;
    }


    /**
     * 得到当前线程维护的Connection对象
     * @return
     */
    private Connection getConnection() {
        Connection conn = null;
        try {
            conn = threadLocalConnection.get();
            if (conn == null || conn.isClosed()) {
                conn = JDBCInstance.getInstance();
                threadLocalConnection.set(conn);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }


    /**
     * 根据维度信息得到维度对应的缓存键
     * 拼接缓存key
     *
     * @param dimension
     * @return
     */
    private String genCacheKey(BaseDimension dimension) {
        StringBuilder sb = new StringBuilder();
        if (dimension instanceof DateDimension) {
            //日期维度
            DateDimension dateDimension = (DateDimension) dimension;
            sb.append("date_dimension")
                    .append(dateDimension.getYear())
                    .append(dateDimension.getMonth())
                    .append(dateDimension.getDay());
        } else if (dimension instanceof ContactDimension) {
            //联系人维度
            ContactDimension contactDimension = (ContactDimension) dimension;
            sb.append("contact_dimension").append(contactDimension.getTelephone());
        }
        return sb.toString();
    }

    /**
     * 返回联系人表的查询和插入语句
     *
     * @return
     */
    private String[] getContactDimensionSQL() {
        String query = "SELECT `id` FROM `tb_contacts` WHERE `telephone` = ? AND `name` = ? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_contacts` (`telephone`, `name`) VALUES (?, ?);";
        return new String[]{query, insert};
    }

    /**
     * 返回时间表的查询和插入语句
     *
     * @return
     */
    private String[] getDateDimensionSQL() {
        String query = "SELECT `id` FROM `tb_dimension_date` WHERE `year` = ? AND `month` = ? AND `day` = ? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_dimension_date` (`year`, `month`, `day`) VALUES (?, ?, ?);";
        return new String[]{query, insert};
    }

    /**
     *
     * @param conn JDBC连接器
     * @param sqls 长度为2，第一个位置为查询语句，第二个位置为插入语句
     * @param dimension 对应维度所保存的数据
     * @return
     */
    private int execSQL(Connection conn, String[] sqls, BaseDimension dimension) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            //1
            //查询的preparedStatement
            preparedStatement = conn.prepareStatement(sqls[0]);
            //根据不同的维度，封装不同的SQL语句
            setArguments(preparedStatement, dimension);
            //执行查询
            resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                int result = resultSet.getInt(1);
                //释放资源
                JDBCUtil.close(null, preparedStatement, resultSet);
                return result;
            }
            //释放资源
            JDBCUtil.close(null, preparedStatement, resultSet);

            //2
            //执行插入，封装插入的sql语句
            preparedStatement = conn.prepareStatement(sqls[1]);
            setArguments(preparedStatement, dimension);
            //执行插入
            preparedStatement.executeUpdate();
            //释放资源
            JDBCUtil.close(null, preparedStatement, null);

            //3
            //查询的preparedStatement
            preparedStatement = conn.prepareStatement(sqls[0]);
            //根据不同的维度，封装不同的SQL语句
            setArguments(preparedStatement, dimension);
            //执行查询
            resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                return resultSet.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            //释放资源
            JDBCUtil.close(null, preparedStatement, resultSet);
        }
        return -1;
    }

    /**
     * 设置SQL语句的具体参数
     * @param preparedStatement
     * @param dimension
     */
    private void setArguments(PreparedStatement preparedStatement, BaseDimension dimension) {
        int i = 0;
        try {
            if (dimension instanceof DateDimension) {
                //可以优化
                DateDimension dateDimension = (DateDimension) dimension;
                preparedStatement.setString(++i, dateDimension.getYear());
                preparedStatement.setString(++i, dateDimension.getMonth());
                preparedStatement.setString(++i, dateDimension.getDay());
            } else if (dimension instanceof ContactDimension) {
                ContactDimension contactDimension = (ContactDimension) dimension;
                preparedStatement.setString(++i, contactDimension.getTelephone());
                preparedStatement.setString(++i, contactDimension.getName());
            }
        } catch (SQLException e) {
           log.error("setArguments Exception", e);
        }
    }

}
