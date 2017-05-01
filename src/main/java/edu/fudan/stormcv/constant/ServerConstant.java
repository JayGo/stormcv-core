package edu.fudan.stormcv.constant;


import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: jkyan
 * Time: 4/30/17 - 8:47 PM
 * Description:
 */
public class ServerConstant {
    public static Map<String, UserInfo> ServerAuthentication;

    static {
        ServerAuthentication = new HashMap<>();
        ServerAuthentication.put("10.134.142.141", new UserInfo("root", "lab369141"));
        ServerAuthentication.put("chick", new UserInfo("root", "lab369141"));
        ServerAuthentication.put("10.134.142.114", new UserInfo("root", "lab369114"));
        ServerAuthentication.put("wolf", new UserInfo("root", "lab369114"));
    }

    public static class UserInfo {
        private String username;
        private String password;

        public UserInfo(String username, String password) {
            this.username = username;
            this.password = password;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UserInfo userInfo = (UserInfo) o;
            if (!username.equals(userInfo.username)) return false;
            return password.equals(userInfo.password);

        }

        @Override
        public int hashCode() {
            int result = username.hashCode();
            result = 31 * result + password.hashCode();
            return result;
        }
    }

}
