package com.baidu.sqlengine.config.model;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.alibaba.druid.wall.WallConfig;
import com.alibaba.druid.wall.WallProvider;
import com.alibaba.druid.wall.spi.MySqlWallProvider;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.config.SqlEngineConfig;
import com.baidu.sqlengine.config.loader.xml.XMLServerLoader;

/**
 * 防火墙配置定义
 */
public final class FirewallConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(FirewallConfig.class);

    private Map<String, List<UserConfig>> whitehost;
    private List<String> blacklist;
    private boolean check = false;

    private WallConfig wallConfig = new WallConfig();

    private static WallProvider provider;

    public FirewallConfig() {
    }

    public void init() {
        if (check) {
            provider = new MySqlWallProvider(wallConfig);
            provider.setBlackListEnable(true);
        }
    }

    public WallProvider getWallProvider() {
        return provider;
    }

    public Map<String, List<UserConfig>> getWhitehost() {
        return this.whitehost;
    }

    public void setWhitehost(Map<String, List<UserConfig>> whitehost) {
        this.whitehost = whitehost;
    }

    public boolean addWhitehost(String host, List<UserConfig> Users) {
        if (existsHost(host)) {
            return false;
        } else {
            this.whitehost.put(host, Users);
            return true;
        }
    }

    public List<String> getBlacklist() {
        return this.blacklist;
    }

    public void setBlacklist(List<String> blacklist) {
        this.blacklist = blacklist;
    }

    public WallProvider getProvider() {
        return provider;
    }

    public boolean existsHost(String host) {
        return this.whitehost != null && whitehost.get(host) != null;
    }

    public boolean canConnect(String host, String user) {
        if (whitehost == null || whitehost.size() == 0) {
            SqlEngineConfig config = SqlEngineServer.getInstance().getConfig();
            Map<String, UserConfig> users = config.getUsers();
            return users.containsKey(user);
        } else {
            List<UserConfig> list = whitehost.get(host);
            if (list == null) {
                return false;
            }
            for (UserConfig userConfig : list) {
                if (userConfig.getName().equals(user)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static void setProvider(WallProvider provider) {
        FirewallConfig.provider = provider;
    }

    public void setWallConfig(WallConfig wallConfig) {
        this.wallConfig = wallConfig;

    }

    public boolean isCheck() {
        return this.check;
    }

    public void setCheck(boolean check) {
        this.check = check;
    }

    public WallConfig getWallConfig() {
        return this.wallConfig;
    }

    public synchronized static void updateToFile(String host, List<UserConfig> userConfigs) throws Exception {
        LOGGER.debug("set white host:" + host + "user:" + userConfigs);
        String filename = SystemConfig.getHomePath() + File.separator + "conf" + File.separator + "server.xml";
        //String filename = "E:\\MyProject\\SqlEngine-Server\\src\\main\\resources\\server.xml";

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(false);
        factory.setValidating(false);
        DocumentBuilder builder = factory.newDocumentBuilder();
        builder.setEntityResolver(new IgnoreDTDEntityResolver());
        Document xmldoc = builder.parse(filename);
        Element whitehost = (Element) xmldoc.getElementsByTagName("whitehost").item(0);
        Element firewall = (Element) xmldoc.getElementsByTagName("firewall").item(0);

        if (firewall == null) {
            firewall = xmldoc.createElement("firewall");
            Element root = xmldoc.getDocumentElement();
            root.appendChild(firewall);
            if (whitehost == null) {
                whitehost = xmldoc.createElement("whitehost");
                firewall.appendChild(whitehost);
            }
        }

        for (UserConfig userConfig : userConfigs) {
            String user = userConfig.getName();
            Element hostEle = xmldoc.createElement("host");
            hostEle.setAttribute("host", host);
            hostEle.setAttribute("user", user);

            whitehost.appendChild(hostEle);
        }

        TransformerFactory factory2 = TransformerFactory.newInstance();
        Transformer former = factory2.newTransformer();
        String systemId = xmldoc.getDoctype().getSystemId();
        if (systemId != null) {
            former.setOutputProperty(javax.xml.transform.OutputKeys.DOCTYPE_SYSTEM, systemId);
        }
        former.transform(new DOMSource(xmldoc), new StreamResult(new File(filename)));

    }

    static class IgnoreDTDEntityResolver implements EntityResolver {
        public InputSource resolveEntity(java.lang.String publicId, java.lang.String systemId)
                throws SAXException, java.io.IOException {
            if (systemId.contains("server.dtd")) {
                InputStream dtd = XMLServerLoader.class.getResourceAsStream("/server.dtd");
                InputSource is = new InputSource(dtd);
                return is;
            } else {
                return null;
            }
        }
    }

}