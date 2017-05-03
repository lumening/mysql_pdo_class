<?php
/**
 * mysql 数据库操作类
 * author: lumening
 * Time: 2016/9/12 16:55
 * mail: luxianfang@live.cn
 *
 * 首先需要说明的是，pdo可以使用三种模式去执行mysql语句，分别是query(),exec(),prepare/execute.
 * query执行返回的是结果集，exec返回的是受影响的行数，prepare/execute返回的是true or false.
 */
class pdo_mysql {
    protected static $_instance = null;
    protected $database = '';
    protected $dbh;
    //$dsn = "mysql：host=服务器地址/名称；port=端口号；dbname=数据库名";
    //$opt = array(PDO::MYSQL_ATTR_INIT_COMMAND=>’set names 连接编码’);
    //$pdo = new pdo($dsn, "用户名", "密码", $opt);

    /**
     * 构造函数
     * @param $server
     * @param $port
     * @param $user
     * @param $password
     * @param $database
     * @param $charset
     */
    private function __construct($server, $port, $user, $password, $database, $charset){
        try{
            $this->dsn = "mysql:host=".$server."; port=".$port."; dbname=".$database;
            $this->opt = array(PDO::MYSQL_ATTR_INIT_COMMAND=>'set names '.$charset);
            $this->dbh = new pdo($this->dsn, $user, $password,$this->opt);
        }catch (PDOException $e){
            $this->outputError($e->getMessage());
        }
    }

    /**
     * 防止克隆
     */
    private function __clone(){}

    /**
     * 单例模式
     * @param $server
     * @param $port
     * @param $user
     * @param $password
     * @param $database
     * @param $charset
     * @return null|pdo_mysql
     */
    public static function getInstance($server, $port, $user, $password, $database, $charset){
        if(self::$_instance===null){
            self::$_instance = new self($server, $port, $user, $password, $database, $charset);
        }
        return self::$_instance;
    }

    /**
     * 查询结果
     * @param $sql
     * @param string $model
     * @param bool $debug
     * @return array|mixed|null
     */
    public function query($sql, $model = "all", $debug=false){
        if($debug===true){
            $this->debug($sql);
        }
        $result = $this->dbh->query($sql);
        if($result){
            $result->setFetchMode(PDO::FETCH_ASSOC);
            if($model=="all"){
                $res = $result->fetchAll();
            }else{
                $res = $result->fetch();
            }
        }else{
            $res = null;
        }
        return $res;
    }

    /**
     * 更新数据
     * @param $table
     * @param $data
     * @param string $where
     * @param $debug
     * @return int
     */
    public function update($table, $data, $where='', $debug=false){
        $this->checkFields($table, $data);
        if($where){
            $sql = "";
            foreach($data as $key=>$val){
                $sql .=", `$key`='$val'";
            }
            $sql = substr($sql, 1);
            $sql = "UPDATE `$table` SET $sql WHERE $where";
        }else{
            $sql = "REPLACE INTO `$table` (`".implode('`,`',array_keys($data))."`) VALUES ('".implode("','",$data)."')";
        }
        if($debug === true){
            $this->debug($sql);
        }
        $res = $this->dbh->prepare($sql);
        $res->execute();
        $this->getPDOError();
        return $res;
    }

    /**
     * 插入数据
     * @param string $table 表名
     * @param array $data  字段名以及字段值
     * @param bool $debug   是否调试
     * @return PDOStatement
     */
    public function insert($table, $data, $debug=false){
        $this->checkFields($table, $data);
        $sql = "INSERT INTO `$table` (`".implode('`,`', array_keys($data))."`) VALUES ('".implode("','",$data)."')";
        if($debug){
            $this->debug($sql);
        }
        $res = $this->dbh->prepare($sql);
        $res->execute();
        $this->getPDOError();
        return $res;
    }

    /**
     * 以覆盖方式插入
     * @param $table
     * @param $data
     * @param bool $debug
     * @return PDOStatement
     */
    public function replace($table, $data, $debug=false){
        $this->checkFields($table, $data);
        $sql = "REPLACE INTO `$table` (`".implode('`,`', array_keys($data))."`) VALUES ('".implode("','",$data)."')";
        if($debug){
            $this->debug($sql);
        }
        $res = $this->dbh->prepare($sql);
        $res->execute();
        $this->getPDOError();
        return $res;
    }

    /**
     * 删除数据
     * @param $table
     * @param $where
     * @param bool $debug
     * @return PDOStatement
     * @throws Exception
     */
    public function delete($table, $where = '', $debug=false){
        if(empty($where)){
            $this->outputError("'WHERE' is null");
        }else{
            $sql = "DELETE from `$table` WHERE $where";
        }
        if($debug){
            $this->debug($sql);
        }
        $res = $this->dbh->prepare($sql);
        $res->execute();
        $this->getPDOError();
        return $res;
    }

    /**
     * 执行sql语句
     * @param $sql
     * @param bool $debug
     * @return int
     */
    public function execSql($sql, $debug=false){
        if($debug){
            $this->debug($sql);
        }
        $res = $this->dbh->exec($sql);
        $this->getPDOError();
        return $res;
    }

    /**
     * 获取指定字段的最大值
     * @param $table
     * @param $field_name
     * @param string $where
     * @param bool $debug
     * @return int
     */
    public function getMaxValue($table, $field_name, $where = '', $debug = false){
        $sql = "SELECT MAX(".$field_name.") FROM `$table`";
        if(!empty($where)){
            $sql .=" WHERE $where";
        }
        if($debug){
            $this->debug($sql);
        }
        $temp  = $this->query($sql,'Row');
        $maxValue = $temp["MAX_VALUE"];
        if(empty($maxValue)){
            $maxValue = 0;
        }
        return $maxValue;
    }

    /**
     * 获取指定列的数量
     * @param $table
     * @param $field_name
     * @param string $where
     * @param bool $debug
     * @return mixed
     */
    public function getCount($table, $field_name, $where = '', $debug = false){
        $sql = "SELECT COUNT(".$field_name.") AS NUM FROM `$table`";
        if(!empty($where)){
            $sql .=" WHERE $where";
        }
        if($debug){
            $this->debug($sql);
        }
        $temp  = $this->query($sql,'Row');
        return $temp["NUM"];
    }

    /**
     * 获取数据表的引擎
     * @param $database
     * @param $table
     * @return mixed
     */
    public function getTableEngine($database, $table){
        $sql = "SHOW TABLE STATUS FROM $database WHERE Name='".$table."'";
        $tableInfo = $this->query($sql);
        return $tableInfo[0]["Engine"];
    }

    /**
     * 事务开始
     */
    private function beginTransaction(){
        $this->dbh->beginTransaction();
    }

    /**
     * 事务提交
     */
    private function commit(){
        $this->dbh->commit();
    }

    /**
     * 事务回滚
     */
    private function rollback(){
        $this->dbh->rollBack();
    }

    /**
     * 执行多条mysql语句，执行前需要通过getTableEngine判断引擎是否支持事务
     * @param $sqls
     * @return bool
     */
    public function execTransaction($sqls){
        $retVal = 1;
        $this->beginTransaction();
        foreach($sqls as $sql){
            if($this->execSql($sql)==0){
                $retVal = 0;
            }
            if($retVal==0){
                $this->rollback();
            }else{
                $this->commit();
                return true;
            }
        }
    }

    /**
     * 检查数据库中是否有存在相应的字段
     * @param $table
     * @param $data
     * @throws Exception
     */
    private function checkFields($table,$data){
        $fields = $this->getFields($table);
        foreach($data as $key=>$val){
            if(!in_array($key,$fields)){
                $this->outputError("没有这个 `$key`");
            }
        }
    }

    /**
     * 获取数据表的字段
     * @param $table
     * @return array
     */
    private function getFields($table){
        $fields = array();
        $result = $this->dbh->query("SHOW COLUMNS FROM $table");
        $this->getPDOError();
        $result->setFetchMode(PDO::FETCH_ASSOC);
        $res = $result->fetchAll();
        foreach($res as $rows){
            $fields[] = $rows['Field'];
        }
        return $fields;
    }

    /**
     * 捕获PDO错误信息
     * @throws Exception
     */
    private function getPDOError(){
        if($this->dbh->errorCode()!='00000'){
            $arrayError = $this->dbh->errorInfo();
            $this->outputError($arrayError[2]);
        }
    }

    /**
     * debug 调试模式
     * @param $info
     */
    private function debug($info){
        var_dump($info);
        exit;
    }

    /**
     * 输出错误信息
     * @param $strErrMsg
     * @throws Exception
     */
    private function outputError($strErrMsg){
        throw new Exception('MySQL Error: '.$strErrMsg);
    }

    /**
     *关闭数据库连接
     */
    public function destruct(){
        $this->dbh = null;
    }
} 
