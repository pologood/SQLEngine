
package com.baidu.sqlengine.config.loader.xml;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLSyntaxErrorException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.baidu.sqlengine.config.model.rule.RuleConfig;
import com.baidu.sqlengine.config.model.rule.TableRuleConfig;
import com.baidu.sqlengine.config.util.ConfigException;
import com.baidu.sqlengine.config.util.ConfigUtil;
import com.baidu.sqlengine.config.util.ParameterMapping;
import com.baidu.sqlengine.route.function.AbstractPartitionAlgorithm;
import com.baidu.sqlengine.util.SplitUtil;

@SuppressWarnings("unchecked")
public class XMLRuleLoader {
    private final static String DEFAULT_DTD = "/dtd/rule.dtd";
    private final static String DEFAULT_XML = "/rule.xml";

    private final Map<String, TableRuleConfig> tableRules;
    // private final Set<RuleConfig> rules;
    private final Map<String, AbstractPartitionAlgorithm> functions;

    public XMLRuleLoader(String ruleFile) {
        // this.rules = new HashSet<RuleConfig>();
        //rule名 -> rule
        this.tableRules = new HashMap<String, TableRuleConfig>();
        //function名 -> 具体分片算法
        this.functions = new HashMap<String, AbstractPartitionAlgorithm>();
        load(DEFAULT_DTD, ruleFile == null ? DEFAULT_XML : ruleFile);
    }

    public XMLRuleLoader() {
        this(null);
    }

    public Map<String, TableRuleConfig> getTableRules() {
        return (Map<String, TableRuleConfig>) (tableRules.isEmpty() ? Collections.emptyMap() : tableRules);
    }

    private void load(String dtdFile, String xmlFile) {
        InputStream dtd = null;
        InputStream xml = null;
        try {
            dtd = XMLRuleLoader.class.getResourceAsStream(dtdFile);
            xml = XMLRuleLoader.class.getResourceAsStream(xmlFile);
            //读取出语意树
            Element root = ConfigUtil.getDocument(dtd, xml).getDocumentElement();
            //加载Function
            loadFunctions(root);
            //加载TableRule
            loadTableRules(root);
        } catch (ConfigException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException(e);
        } finally {
            if (dtd != null) {
                try {
                    dtd.close();
                } catch (IOException e) {
                }
            }
            if (xml != null) {
                try {
                    xml.close();
                } catch (IOException e) {
                }
            }
        }
    }

    /**
     * tableRule标签结构：
     * <tableRule name="sharding-by-month">
     * <rule>
     * <columns>create_date</columns>
     * <algorithm>partbymonth</algorithm>
     * </rule>
     * </tableRule>
     *
     * @param root
     *
     * @throws SQLSyntaxErrorException
     */
    private void loadTableRules(Element root) throws SQLSyntaxErrorException {
        //获取每个tableRule标签
        NodeList list = root.getElementsByTagName("tableRule");
        for (int i = 0, n = list.getLength(); i < n; ++i) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                //先判断是否重复
                String name = e.getAttribute("name");
                if (tableRules.containsKey(name)) {
                    throw new ConfigException("table rule " + name
                            + " duplicated!");
                }
                //获取rule标签
                NodeList ruleNodes = e.getElementsByTagName("rule");
                int length = ruleNodes.getLength();
                if (length > 1) {
                    throw new ConfigException("only one rule can defined :"
                            + name);
                }
                //目前只处理第一个，未来可能有多列复合逻辑需求
                //RuleConfig是保存着rule与function对应关系的对象
                RuleConfig rule = loadRule((Element) ruleNodes.item(0));
                String funName = rule.getFunctionName();
                //判断function是否存在，获取function
                AbstractPartitionAlgorithm func = functions.get(funName);
                if (func == null) {
                    throw new ConfigException("can't find function of name :"
                            + funName);
                }
                rule.setRuleAlgorithm(func);
                //保存到tableRules
                tableRules.put(name, new TableRuleConfig(name, rule));
            }
        }
    }

    private RuleConfig loadRule(Element element) throws SQLSyntaxErrorException {
        //读取columns
        Element columnsEle = ConfigUtil.loadElement(element, "columns");
        String column = columnsEle.getTextContent();
        String[] columns = SplitUtil.split(column, ',', true);
        if (columns.length > 1) {
            throw new ConfigException("table rule coulmns has multi values:"
                    + columnsEle.getTextContent());
        }
        //读取algorithm
        Element algorithmEle = ConfigUtil.loadElement(element, "algorithm");
        String algorithm = algorithmEle.getTextContent();
        return new RuleConfig(column.toUpperCase(), algorithm);
    }

    /**
     * function标签结构：
     * <function name="partbymonth" class="com.baidu.sqlengine.route.function.PartitionByMonth">
     * <property name="dateFormat">yyyy-MM-dd</property>
     * <property name="sBeginDate">2015-01-01</property>
     * </function>
     *
     * @param root
     *
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    private void loadFunctions(Element root) throws ClassNotFoundException,
            InstantiationException, IllegalAccessException,
            InvocationTargetException {
        NodeList list = root.getElementsByTagName("function");
        for (int i = 0, n = list.getLength(); i < n; ++i) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                //获取name标签
                String name = e.getAttribute("name");
                //如果Map已有，则function重复
                if (functions.containsKey(name)) {
                    throw new ConfigException("rule function " + name
                            + " duplicated!");
                }
                //获取class标签
                String clazz = e.getAttribute("class");
                //根据class利用反射新建分片算法
                AbstractPartitionAlgorithm function = createFunction(name, clazz);
                //根据读取参数配置分片算法
                ParameterMapping.mapping(function, ConfigUtil.loadElements(e));
                //每个AbstractPartitionAlgorithm可能会实现init来初始化
                function.init();
                //放入functions map
                functions.put(name, function);
            }
        }
    }

    private AbstractPartitionAlgorithm createFunction(String name, String clazz)
            throws ClassNotFoundException, InstantiationException,
            IllegalAccessException, InvocationTargetException {
        Class<?> clz = Class.forName(clazz);
        //判断是否继承AbstractPartitionAlgorithm
        if (!AbstractPartitionAlgorithm.class.isAssignableFrom(clz)) {
            throw new IllegalArgumentException("rule function must implements "
                    + AbstractPartitionAlgorithm.class.getName() + ", name=" + name);
        }
        return (AbstractPartitionAlgorithm) clz.newInstance();
    }

}