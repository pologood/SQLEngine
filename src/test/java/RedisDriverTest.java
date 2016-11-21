import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.baidu.sqlengine.backend.jdbc.redis.RedisDriver;

/**
 * Created by wangchongjie on 16/11/18.
 */
public class RedisDriverTest {

    public static void main(String[] args){
        RedisDriver driver = new RedisDriver();
        String url = "redis://111.111.111.111:22";
        try {
            Connection conn = driver.connect(url, null);
            Statement stat = conn.createStatement();
            ResultSet rs = stat.executeQuery("select val1 from tbl where key = key1");
            System.out.println(rs.getString(1));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
