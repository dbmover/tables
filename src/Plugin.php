<?php

/**
 * @package Dbmover
 * @subpackage Tables
 */

namespace Dbmover\Tables;

use Dbmover\Core;
use PDO;

class Plugin extends Core\Plugin
{
    public function __invoke(string $sql) : string
    {
        $tables = [];
        $exists = $this->loader->getPdo()->prepare(
            "SELECT * FROM INFORMATION_SCHEMA.TABLES
                WHERE ((TABLE_CATALOG = ? AND TABLE_SCHEMA = 'public') OR TABLE_SCHEMA = ?)
                AND TABLE_TYPE = 'BASE TABLE'
                AND TABLE_NAME = ?");
        if (preg_match_all("@CREATE.*?TABLE\s*([^\s]+)\s*\((.*)^\).*?;$@ms", $sql, $matches, PREG_SET_ORDER)) {
            foreach ($matches as $table) {
                $tables[] = $table[1];
                // If no such table exists, create verbatim.
                $exists->execute([$this->loader->getDatabase(), $this->loader->getDatabase(), $table[1]]);
                if (false === $exists->fetch(PDO::FETCH_ASSOC)) {
                    $this->loader->addOperation($table[0]);
                } else {
                    // Check if the table needs modifications.
                }
                $sql = str_replace($table[0], '', $sql);
            }
        }

        // Drop tables that are no longer needed.
        $exists = $this->loader->getPdo()->prepare(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                WHERE ((TABLE_CATALOG = ? AND TABLE_SCHEMA = 'public') OR TABLE_SCHEMA = ?)
                AND TABLE_TYPE = 'BASE TABLE'");
        $exists->execute([$this->loader->getDatabase(), $this->loader->getDatabase()]);
        while (false !== ($table = $exists->fetchColumn())) {
            if (!in_array($table, $tables)) {
                $this->loader->addOperation("DROP TABLE $table;");
            }
        }
        return $sql;
    }

    /*
    public function __destruct()
    {


        $class = new static($extr[1], $parent);
        $columnClass = $class->getObjectName('Column');
        $indexClass = $class->getObjectName('Index');
        $lines = preg_split('@,$@m', rtrim($extr[2]));
        $class->current = (object)['columns' => [], 'indexes' => []];
        foreach ($lines as $line) {
            $line = trim($line);
            preg_match('@^\w+@', $line, $name);
            $class->current->columns[$name[0]] = $columnClass::fromSql($line, $class);
            if (stripos($line, 'AUTO_INCREMENT')) {
                $class->current->indexes[$name[0]] = $indexClass::fromSql($line, $class);
            }
        }
        return $class;
    }
    public function setCurrentState(PDO $pdo, string $database)
    {
        if (!isset(self::$columns)) {
            self::$columns = $pdo->prepare(
                "SELECT
                    COLUMN_NAME colname
                FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE (TABLE_CATALOG = ? OR TABLE_SCHEMA = ?) AND TABLE_NAME = ?
                    ORDER BY ORDINAL_POSITION ASC"
            );
        }
        self::$columns->execute([$database, $database, $this->name]);
        $this->current = (object)['columns' => [], 'indexes' => []];
        $this->setCurrentIndexes($pdo, $database);
        $cols = [];
        $class = $this->getObjectName('Column');
        foreach (self::$columns->fetchAll(PDO::FETCH_ASSOC) as $column) {
            $this->current->columns[$column['colname']] = new $class($column['colname'], $this);
            $this->current->columns[$column['colname']]->setCurrentState($pdo, $database);
        }
    }

    //protected abstract function setCurrentIndexes(PDO $pdo, string $database);

    public function toSql() : array
    {
        $operations = [];
        foreach (['columns', 'indexes'] as $type) {
            foreach ($this->current->$type as $obj) {
                if (isset($this->requested->current->$type[$obj->name])) {
                    $obj->setComparisonObject($this->requested->current->$type[$obj->name]);
                }
                $operations = array_merge($operations, $obj->toSql());
            }
            foreach ($this->requested->current->$type as $obj) {
                if (!isset($this->current->$type[$obj->name])) {
                    $class = get_class($obj);
                    $newobj = new $class($obj->name, $obj->parent);
                    $newobj->setComparisonObject($obj);
                    $operations = array_merge($operations, $newobj->toSql());
                }
            }
        }
        return $operations;
    }
    */
}

